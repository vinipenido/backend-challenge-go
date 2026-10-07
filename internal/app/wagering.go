package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"github.com/vinipenido/backend-challenge-go/internal/domain/event"
	"github.com/vinipenido/backend-challenge-go/internal/domain/money"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wager"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wallet"
)

// Channel identifica a origem do processamento (rótulo de métricas e logs).
type Channel string

// Canais de entrada.
const (
	ChannelHTTP   Channel = "http"
	ChannelSQS    Channel = "sqs"
	ChannelWorker Channel = "worker"
)

// WageringConfig parametriza o processamento.
type WageringConfig struct {
	// MaxTxAttempts limita as repetições da transação SQL em conflitos de concorrência.
	MaxTxAttempts int
	// PendingReferenceTTL é o prazo máximo de espera por uma referência.
	PendingReferenceTTL time.Duration
	// PendingReferenceMaxAttempts limita as novas tentativas do worker.
	PendingReferenceMaxAttempts int
	// PendingBackoffBase/Max definem o backoff exponencial do worker.
	PendingBackoffBase time.Duration
	PendingBackoffMax  time.Duration
}

// ProcessCommand é a entrada do caso de uso, idêntica para HTTP e SQS.
type ProcessCommand struct {
	Request       WagerRequest
	Channel       Channel
	CorrelationID string
	CausationID   string
	// Inbox é preenchido apenas na entrada por SQS.
	Inbox *InboxMessage
}

// ProcessResult é o resultado persistido devolvido ao chamador.
type ProcessResult struct {
	Transaction wager.Snapshot
	// Replay indica que nenhuma operação foi aplicada agora: o resultado é o
	// registrado anteriormente para a mesma chave/conteúdo.
	Replay bool
	// DuplicateMessage indica reentrega de uma mensagem já concluída (inbox).
	DuplicateMessage bool
}

// WageringService implementa o processamento de operações externas.
type WageringService struct {
	uow     UnitOfWork
	clock   Clock
	ids     IDGenerator
	metrics Metrics
	log     *slog.Logger
	cfg     WageringConfig
}

// NewWageringService cria o serviço.
func NewWageringService(uow UnitOfWork, clock Clock, ids IDGenerator, metrics Metrics, log *slog.Logger, cfg WageringConfig) *WageringService {
	if cfg.MaxTxAttempts <= 0 {
		cfg.MaxTxAttempts = 5
	}
	if cfg.PendingBackoffBase <= 0 {
		cfg.PendingBackoffBase = time.Second
	}
	if cfg.PendingBackoffMax <= 0 {
		cfg.PendingBackoffMax = 30 * time.Second
	}
	if cfg.PendingReferenceTTL <= 0 {
		cfg.PendingReferenceTTL = 5 * time.Minute
	}
	if cfg.PendingReferenceMaxAttempts <= 0 {
		cfg.PendingReferenceMaxAttempts = 20
	}
	return &WageringService{uow: uow, clock: clock, ids: ids, metrics: metrics, log: log, cfg: cfg}
}

// Process aplica a operação de forma idempotente. Toda a decisão financeira,
// o registro da transação, o ledger, a outbox e a inbox são confirmados em uma
// única transação SQL. Conflitos de concorrência repetem a transação inteira.
func (s *WageringService) Process(ctx context.Context, cmd ProcessCommand) (ProcessResult, error) {
	start := time.Now()
	req := cmd.Request
	log := s.log.With(
		slog.String("correlationId", cmd.CorrelationID),
		slog.String("providerId", req.ProviderID),
		slog.String("walletId", req.WalletID.String()),
		slog.String("kind", string(req.Kind)),
	)
	if cmd.Inbox != nil {
		log = log.With(slog.String("messageId", cmd.Inbox.MessageID))
	}

	var (
		res     ProcessResult
		created *wager.Transaction
		err     error
	)
	for attempt := 1; ; attempt++ {
		res, created, err = s.processOnce(ctx, cmd)
		reason, conflict := IsConcurrencyConflict(err)
		if !conflict {
			break
		}
		s.metrics.ConcurrencyConflict(reason)
		if attempt >= s.cfg.MaxTxAttempts {
			log.Warn("conflitos de concorrência esgotaram as tentativas", slog.String("reason", reason))
			err = Transient(err)
			break
		}
		s.metrics.Retry("wager_tx")
		if werr := sleepCtx(ctx, backoffJitter(attempt, 5*time.Millisecond, 200*time.Millisecond)); werr != nil {
			err = Transient(werr)
			break
		}
	}

	switch {
	case err == nil:
	case IsTransient(err):
		log.Warn("falha transitória ao processar operação", slog.String("error", err.Error()))
		return ProcessResult{}, err
	case isBusinessError(err):
		if errors.Is(err, ErrIdempotencyConflict) || errors.Is(err, ErrExternalTransactionExists) {
			s.metrics.IdempotencyConflict(string(cmd.Channel))
		}
		log.Info("operação recusada", slog.String("reason", err.Error()))
		return ProcessResult{}, err
	default:
		// Falha permanente inesperada: registra FAILED para auditoria, sem efeito financeiro.
		log.Error("falha permanente ao processar operação", slog.String("error", err.Error()))
		failed, ferr := s.recordFailure(ctx, cmd)
		if ferr != nil {
			log.Error("não foi possível registrar a falha permanente", slog.String("error", ferr.Error()))
			return ProcessResult{}, fmt.Errorf("%w: %v", ErrPermanentFailure, err)
		}
		s.metrics.TransactionOutcome(string(cmd.Channel), string(req.Kind), string(wager.StatusFailed), false, time.Since(start))
		return ProcessResult{Transaction: failed}, fmt.Errorf("%w: %v", ErrPermanentFailure, err)
	}

	snap := res.Transaction
	log = log.With(slog.String("transactionId", snap.ID.String()))
	if res.Replay {
		reason := "idempotent_replay"
		if res.DuplicateMessage {
			reason = "inbox_duplicate"
		}
		s.metrics.Duplicate(string(cmd.Channel), reason)
		log.Info("replay idempotente", slog.String("status", string(snap.Status)), slog.String("reason", reason))
	} else {
		log.Info("operação registrada", slog.String("status", string(snap.Status)), slog.String("failureCode", string(snap.FailureCode)))
	}
	s.metrics.TransactionOutcome(string(cmd.Channel), string(req.Kind), string(snap.Status), res.Replay, time.Since(start))

	// Após o COMMIT: antecipa operações que aguardavam esta referência.
	if created != nil {
		s.wakeDependents(ctx, created.Snapshot(), log)
	}
	return res, nil
}

func isBusinessError(err error) bool {
	var ie *wager.InputError
	return errors.As(err, &ie) ||
		errors.Is(err, ErrWalletNotFound) ||
		errors.Is(err, ErrIdempotencyConflict) ||
		errors.Is(err, ErrExternalTransactionExists) ||
		errors.Is(err, ErrInboxHashConflict) ||
		errors.Is(err, ErrUntrustedProvider)
}

func (s *WageringService) processOnce(ctx context.Context, cmd ProcessCommand) (ProcessResult, *wager.Transaction, error) {
	var (
		res     ProcessResult
		created *wager.Transaction
	)
	req := cmd.Request
	err := s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		res, created = ProcessResult{}, nil

		// 1. Inbox: reentrega da mesma mensagem devolve o resultado registrado.
		if cmd.Inbox != nil {
			prev, err := st.Inbox().Get(ctx, cmd.Inbox.Consumer, cmd.Inbox.MessageID)
			if err != nil {
				return err
			}
			if prev != nil {
				if prev.Hash != cmd.Inbox.Hash {
					return ErrInboxHashConflict
				}
				res.Replay, res.DuplicateMessage = true, true
				if prev.TransactionID != nil {
					tx, err := st.Transactions().Get(ctx, *prev.TransactionID)
					if err != nil {
						return err
					}
					res.Transaction = tx.Snapshot()
				}
				return nil
			}
		}

		// 2. Idempotência antes do lock (replays não disputam a carteira).
		if replay, err := s.lookupIdempotent(ctx, st, req); err != nil || replay != nil {
			if err != nil {
				return err
			}
			res = ProcessResult{Transaction: replay.Snapshot(), Replay: true}
			return s.recordInbox(ctx, st, cmd, replay.ID())
		}

		// 3. Lock pessimista da carteira: serializa apenas operações da mesma carteira.
		w, err := st.Wallets().GetForUpdate(ctx, req.WalletID)
		if err != nil {
			return err
		}

		// 4. Revalida a idempotência sob o lock (outro escritor pode ter confirmado).
		if replay, err := s.lookupIdempotent(ctx, st, req); err != nil || replay != nil {
			if err != nil {
				return err
			}
			res = ProcessResult{Transaction: replay.Snapshot(), Replay: true}
			return s.recordInbox(ctx, st, cmd, replay.ID())
		}

		now := s.clock.Now()
		tx, err := wager.NewExternal(wager.ExternalParams{
			ID:                             s.ids.New(),
			ProviderID:                     req.ProviderID,
			ExternalTransactionID:          req.ExternalTransactionID,
			IdempotencyKey:                 req.IdempotencyKey,
			PayloadHash:                    req.PayloadHash,
			WalletID:                       req.WalletID,
			PlayerID:                       req.PlayerID,
			RoundID:                        req.RoundID,
			GameID:                         req.GameID,
			Kind:                           req.Kind,
			Money:                          req.Money,
			ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
			CorrelationID:                  cmd.CorrelationID,
			CausationID:                    cmd.CausationID,
			Now:                            now,
		})
		if err != nil {
			return err
		}
		if err := s.applyAndPersist(ctx, st, w, tx, true, now); err != nil {
			return err
		}
		res = ProcessResult{Transaction: tx.Snapshot()}
		created = tx
		return s.recordInbox(ctx, st, cmd, tx.ID())
	})
	return res, created, err
}

// lookupIdempotent devolve a transação existente para a mesma chave (replay),
// ou erro de conflito quando a chave ou a operação já foram usadas de outra forma.
func (s *WageringService) lookupIdempotent(ctx context.Context, st Store, req WagerRequest) (*wager.Transaction, error) {
	// Uma única consulta: em READ COMMITTED, duas consultas separadas poderiam
	// enxergar estados diferentes se outra entrada confirmasse entre elas.
	found, err := st.Transactions().FindIdempotencyCandidates(ctx, req.ProviderID, req.IdempotencyKey, req.ExternalTransactionID)
	if err != nil {
		return nil, err
	}
	var byExt *wager.Transaction
	for _, tx := range found {
		if tx.Snapshot().IdempotencyKey == req.IdempotencyKey {
			if tx.PayloadHash() != req.PayloadHash {
				return nil, ErrIdempotencyConflict
			}
			return tx, nil
		}
		byExt = tx
	}
	if byExt != nil {
		return nil, ErrExternalTransactionExists
	}
	return nil, nil
}

func (s *WageringService) recordInbox(ctx context.Context, st Store, cmd ProcessCommand, txID uuid.UUID) error {
	if cmd.Inbox == nil {
		return nil
	}
	m := *cmd.Inbox
	id := txID
	m.TransactionID = &id
	m.ProcessedAt = s.clock.Now()
	return st.Inbox().Record(ctx, m)
}

// applyAndPersist decide o resultado (com a carteira travada), aplica o
// movimento e grava transação, carteira, ledger e eventos na mesma transação SQL.
func (s *WageringService) applyAndPersist(ctx context.Context, st Store, w *wallet.Wallet, tx *wager.Transaction, isNew bool, now time.Time) error {
	prevStatus := tx.Status()
	balance := w.Balance()
	entry, err := s.decide(ctx, st, w, tx, now)
	if err != nil {
		return err
	}
	if isNew {
		if err := st.Transactions().Insert(ctx, tx.Snapshot()); err != nil {
			return err
		}
	} else if err := st.Transactions().Update(ctx, tx.Snapshot(), prevStatus); err != nil {
		return err
	}
	if entry != nil {
		if err := st.Wallets().UpdateBalance(ctx, w, w.Version()-1); err != nil {
			return err
		}
		if err := st.Ledger().Insert(ctx, *entry); err != nil {
			return err
		}
	} else if !w.Balance().Equal(balance) {
		return fmt.Errorf("saldo alterado sem lançamento (invariante violada)")
	}
	events := append(w.PullEvents(), tx.PullEvents()...)
	return st.Outbox().Append(ctx, events...)
}

// decide avalia regras de carteira e referência e executa a transição de estado.
// Devolve o lançamento de ledger quando há movimentação.
func (s *WageringService) decide(ctx context.Context, st Store, w *wallet.Wallet, tx *wager.Transaction, now time.Time) (*wallet.LedgerEntry, error) {
	balance := w.Balance()
	if tx.PlayerID() != w.PlayerID() {
		return nil, tx.MarkRejected(wager.FailurePlayerWalletMismatch, &balance, nil, now)
	}
	if tx.Money().Currency() != w.Currency() {
		return nil, tx.MarkRejected(wager.FailureCurrencyMismatch, &balance, nil, now)
	}

	var refState *wager.ReferenceState
	if refExt := tx.ReferenceExternalID(); refExt != "" {
		ref, err := st.Transactions().FindByExternalID(ctx, tx.ProviderID(), refExt)
		if err != nil {
			return nil, err
		}
		if ref != nil {
			reversed := false
			if tx.Kind().IsReversal() {
				if reversed, err = st.Transactions().HasProcessedReversal(ctx, ref.ID()); err != nil {
					return nil, err
				}
			}
			refState = &wager.ReferenceState{Snapshot: ref.Snapshot(), HasProcessedReversal: reversed}
		}
	}

	d := tx.Decide(refState)
	switch d.Outcome {
	case wager.OutcomeWait:
		return nil, s.waitOrExpire(tx, refState, &balance, now)
	case wager.OutcomeReject:
		return nil, tx.MarkRejected(d.FailureCode, &balance, d.ReferenceID, now)
	}

	if d.Movement == wager.MovementNone {
		return nil, tx.MarkProcessed(balance, d.ReferenceID, now)
	}
	mv := wallet.Movement{
		TransactionID: tx.ID(),
		EntryID:       s.ids.New(),
		Amount:        tx.Money(),
		Meta:          event.Metadata{CorrelationID: tx.Snapshot().CorrelationID, OccurredAt: now},
	}
	var (
		entry wallet.LedgerEntry
		err   error
	)
	if d.Movement == wager.MovementDebit {
		entry, err = w.Debit(mv)
	} else {
		entry, err = w.Credit(mv)
	}
	if errors.Is(err, wallet.ErrInsufficientFunds) {
		return nil, tx.MarkRejected(d.InsufficientFundsCode, &balance, d.ReferenceID, now)
	}
	if err != nil {
		return nil, err
	}
	if err := tx.MarkProcessed(w.Balance(), d.ReferenceID, now); err != nil {
		return nil, err
	}
	return &entry, nil
}

// waitOrExpire agenda nova tentativa (backoff exponencial) ou finaliza como
// REJECTED quando o TTL ou o máximo de tentativas foi atingido.
func (s *WageringService) waitOrExpire(tx *wager.Transaction, ref *wager.ReferenceState, balance *money.Money, now time.Time) error {
	if tx.Status() == wager.StatusPendingReference {
		expired := (tx.ExpiresAt() != nil && !now.Before(*tx.ExpiresAt())) ||
			tx.Attempts()+1 >= s.cfg.PendingReferenceMaxAttempts
		if expired {
			code := wager.FailureReferenceNotFound
			if ref != nil {
				code = wager.FailureReferenceStillPending
			}
			return tx.MarkRejected(code, balance, nil, now)
		}
	}
	next := now.Add(s.pendingBackoff(tx.Attempts()))
	expires := now.Add(s.cfg.PendingReferenceTTL)
	if tx.ExpiresAt() != nil && next.After(*tx.ExpiresAt()) {
		next = *tx.ExpiresAt()
	}
	return tx.MarkPendingReference(next, expires, now)
}

func (s *WageringService) pendingBackoff(attempts int) time.Duration {
	d := s.cfg.PendingBackoffBase
	for i := 0; i < attempts && d < s.cfg.PendingBackoffMax; i++ {
		d *= 2
	}
	return min(d, s.cfg.PendingBackoffMax)
}

func (s *WageringService) wakeDependents(ctx context.Context, snap wager.Snapshot, log *slog.Logger) {
	if snap.Origin != wager.OriginExternal || snap.Status == wager.StatusPendingReference {
		return
	}
	err := s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		_, err := st.Transactions().WakeDependents(ctx, snap.ProviderID, snap.ExternalTransactionID, s.clock.Now())
		return err
	})
	if err != nil {
		log.Debug("não foi possível antecipar dependentes (o worker fará a retomada)", slog.String("error", err.Error()))
	}
}

// recordFailure registra a operação como FAILED em transação própria, sem
// efeito financeiro, para auditoria e para que replays devolvam o mesmo resultado.
func (s *WageringService) recordFailure(ctx context.Context, cmd ProcessCommand) (wager.Snapshot, error) {
	req := cmd.Request
	var snap wager.Snapshot
	err := s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		now := s.clock.Now()
		tx, err := wager.NewExternal(wager.ExternalParams{
			ID: s.ids.New(), ProviderID: req.ProviderID, ExternalTransactionID: req.ExternalTransactionID,
			IdempotencyKey: req.IdempotencyKey, PayloadHash: req.PayloadHash, WalletID: req.WalletID,
			PlayerID: req.PlayerID, RoundID: req.RoundID, GameID: req.GameID, Kind: req.Kind, Money: req.Money,
			ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
			CorrelationID:                  cmd.CorrelationID, CausationID: cmd.CausationID, Now: now,
		})
		if err != nil {
			return err
		}
		if err := tx.MarkFailed(wager.FailureInternalPermanent, now); err != nil {
			return err
		}
		if err := st.Transactions().Insert(ctx, tx.Snapshot()); err != nil {
			return err
		}
		snap = tx.Snapshot()
		return s.recordInbox(ctx, st, cmd, tx.ID())
	})
	return snap, err
}

// ResolvePending retoma pendências vencidas (PENDING ou PENDING_REFERENCE).
// Pode rodar em várias instâncias: o lock da carteira e o FOR UPDATE SKIP
// LOCKED na transação garantem que cada pendência é tratada uma única vez.
func (s *WageringService) ResolvePending(ctx context.Context, limit int) (int, error) {
	due, err := s.listDue(ctx, limit)
	if err != nil {
		return 0, err
	}
	handled := 0
	for _, ref := range due {
		if ctx.Err() != nil {
			return handled, ctx.Err()
		}
		ok, err := s.resolveOne(ctx, ref)
		if err != nil {
			if reason, conflict := IsConcurrencyConflict(err); conflict {
				s.metrics.ConcurrencyConflict(reason)
				continue
			}
			return handled, err
		}
		if ok {
			handled++
		}
	}
	return handled, nil
}

func (s *WageringService) listDue(ctx context.Context, limit int) ([]PendingRef, error) {
	var due []PendingRef
	err := s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		var err error
		due, err = st.Transactions().ListDuePending(ctx, s.clock.Now(), limit)
		return err
	})
	return due, err
}

func (s *WageringService) resolveOne(ctx context.Context, ref PendingRef) (bool, error) {
	start := time.Now()
	var (
		snap    wager.Snapshot
		handled bool
	)
	err := s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		handled = false
		// Mesma ordem de locks do caminho síncrono: carteira e depois transação.
		w, err := st.Wallets().GetForUpdate(ctx, ref.WalletID)
		if err != nil {
			return err
		}
		now := s.clock.Now()
		tx, err := st.Transactions().LockDuePending(ctx, ref.ID, now)
		if err != nil || tx == nil {
			return err
		}
		if err := s.applyAndPersist(ctx, st, w, tx, false, now); err != nil {
			return err
		}
		snap, handled = tx.Snapshot(), true
		return nil
	})
	if err != nil || !handled {
		return false, err
	}
	log := s.log.With(
		slog.String("transactionId", snap.ID.String()), slog.String("walletId", snap.WalletID.String()),
		slog.String("providerId", snap.ProviderID), slog.String("correlationId", snap.CorrelationID),
	)
	if snap.Status == wager.StatusPendingReference {
		s.metrics.Retry("pending_reference")
		log.Debug("referência ainda indisponível; nova tentativa agendada", slog.Int("attempts", snap.Attempts))
		return true, nil
	}
	log.Info("pendência resolvida", slog.String("status", string(snap.Status)), slog.String("failureCode", string(snap.FailureCode)))
	s.metrics.TransactionOutcome(string(ChannelWorker), string(snap.Kind), string(snap.Status), false, time.Since(start))
	s.wakeDependents(ctx, snap, log)
	return true, nil
}

func backoffJitter(attempt int, base, max time.Duration) time.Duration {
	d := base << min(attempt, 10)
	if d > max {
		d = max
	}
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
