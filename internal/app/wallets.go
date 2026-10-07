package app

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/vinipenido/backend-challenge-go/internal/domain/event"
	"github.com/vinipenido/backend-challenge-go/internal/domain/money"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wager"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wallet"
)

// WalletService reúne os casos de uso internos de carteira.
type WalletService struct {
	uow     UnitOfWork
	clock   Clock
	ids     IDGenerator
	metrics Metrics
	log     *slog.Logger
}

// NewWalletService cria o serviço.
func NewWalletService(uow UnitOfWork, clock Clock, ids IDGenerator, metrics Metrics, log *slog.Logger) *WalletService {
	return &WalletService{uow: uow, clock: clock, ids: ids, metrics: metrics, log: log}
}

// OpenWalletCommand é a entrada da abertura de carteira.
type OpenWalletCommand struct {
	PlayerID       string
	InitialBalance *MoneyInput
	CorrelationID  string
}

// WalletView é a visão de leitura de uma carteira.
type WalletView struct {
	ID        uuid.UUID
	PlayerID  uuid.UUID
	Balance   money.Money
	Version   int64
	CreatedAt string
	UpdatedAt string
}

func viewOf(w *wallet.Wallet) WalletView {
	return WalletView{
		ID: w.ID(), PlayerID: w.PlayerID(), Balance: w.Balance(), Version: w.Version(),
		CreatedAt: event.FormatTime(w.CreatedAt()), UpdatedAt: event.FormatTime(w.UpdatedAt()),
	}
}

// Open cria a carteira. Com saldo positivo, a transação OPENING (PROCESSED), o
// lançamento de crédito e os eventos WagerTransactionProcessed e
// WalletBalanceChanged são confirmados no mesmo COMMIT da carteira.
func (s *WalletService) Open(ctx context.Context, cmd OpenWalletCommand) (WalletView, error) {
	playerID, err := ParseUUID("playerId", cmd.PlayerID)
	if err != nil {
		return WalletView{}, err
	}
	initial, err := ParseMoneyInput("initialBalance", cmd.InitialBalance)
	if err != nil {
		return WalletView{}, err
	}
	now := s.clock.Now()
	walletID, openingID := s.ids.New(), s.ids.New()
	w, entry, err := wallet.Open(wallet.OpenParams{
		ID: walletID, PlayerID: playerID, InitialBalance: initial,
		OpeningTransactionID: openingID, OpeningEntryID: s.ids.New(),
		Meta: event.Metadata{CorrelationID: cmd.CorrelationID, OccurredAt: now},
	})
	if err != nil {
		return WalletView{}, fmt.Errorf("%w: %v", wager.ErrInvalidRequest, err)
	}

	err = s.uow.Do(ctx, func(ctx context.Context, st Store) error {
		if err := st.Wallets().Insert(ctx, w); err != nil {
			return err
		}
		events := w.PullEvents()
		if entry != nil {
			opening, err := wager.NewOpening(openingID, walletID, playerID, initial, cmd.CorrelationID, now)
			if err != nil {
				return err
			}
			if err := opening.MarkProcessed(initial, nil, now); err != nil {
				return err
			}
			if err := st.Transactions().Insert(ctx, opening.Snapshot()); err != nil {
				return err
			}
			if err := st.Ledger().Insert(ctx, *entry); err != nil {
				return err
			}
			events = append(events, opening.PullEvents()...)
		}
		return st.Outbox().Append(ctx, events...)
	})
	if err != nil {
		return WalletView{}, err
	}
	s.log.Info("carteira aberta", slog.String("walletId", walletID.String()),
		slog.String("correlationId", cmd.CorrelationID), slog.Bool("openingCredit", entry != nil))
	return viewOf(w), nil
}

// Get devolve a carteira.
func (s *WalletService) Get(ctx context.Context, id uuid.UUID) (WalletView, error) {
	var view WalletView
	err := s.uow.ReadSnapshot(ctx, func(ctx context.Context, st Store) error {
		w, err := st.Wallets().Get(ctx, id)
		if err != nil {
			return err
		}
		view = viewOf(w)
		return nil
	})
	return view, err
}

// LedgerPage é uma página do ledger.
type LedgerPage struct {
	WalletID   uuid.UUID
	Entries    []wallet.LedgerEntry
	NextCursor string
}

type ledgerCursor struct {
	WalletID string `json:"w"`
	After    int64  `json:"v"`
}

// EncodeLedgerCursor gera o cursor opaco (base64url de JSON), vinculado à carteira.
func EncodeLedgerCursor(walletID uuid.UUID, afterVersion int64) string {
	b, _ := json.Marshal(ledgerCursor{WalletID: walletID.String(), After: afterVersion})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeLedgerCursor(walletID uuid.UUID, cursor string) (int64, error) {
	if cursor == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, ErrInvalidCursor
	}
	var c ledgerCursor
	if err := json.Unmarshal(raw, &c); err != nil || c.WalletID != walletID.String() || c.After < 0 {
		return 0, ErrInvalidCursor
	}
	return c.After, nil
}

// ListLedger pagina o ledger em ordem estável (versão da carteira crescente).
func (s *WalletService) ListLedger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) (LedgerPage, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	after, err := decodeLedgerCursor(walletID, cursor)
	if err != nil {
		return LedgerPage{}, err
	}
	page := LedgerPage{WalletID: walletID}
	err = s.uow.ReadSnapshot(ctx, func(ctx context.Context, st Store) error {
		if _, err := st.Wallets().Get(ctx, walletID); err != nil {
			return err
		}
		entries, err := st.Ledger().List(ctx, walletID, after, limit+1)
		if err != nil {
			return err
		}
		if len(entries) > limit {
			entries = entries[:limit]
			page.NextCursor = EncodeLedgerCursor(walletID, entries[len(entries)-1].WalletVersion())
		}
		page.Entries = entries
		return nil
	})
	return page, err
}

// Reconciliation é o resultado da reconciliação.
type Reconciliation struct {
	WalletID          uuid.UUID
	StoredBalance     money.Money
	CalculatedBalance money.Money
	Difference        money.Money
	Consistent        bool
	CheckedEntries    int64
}

// Reconcile reconstrói o saldo a partir do ledger (créditos - débitos, incluindo
// a abertura) em uma visão consistente (REPEATABLE READ) e compara com o saldo
// armazenado. Não altera dados; divergências vão para resposta, log e métrica.
func (s *WalletService) Reconcile(ctx context.Context, walletID uuid.UUID, correlationID string) (Reconciliation, error) {
	var r Reconciliation
	err := s.uow.ReadSnapshot(ctx, func(ctx context.Context, st Store) error {
		w, err := st.Wallets().Get(ctx, walletID)
		if err != nil {
			return err
		}
		totals, err := st.Ledger().Totals(ctx, walletID)
		if err != nil {
			return err
		}
		credits, err := money.FromMinor(totals.CreditsMinor, w.Currency())
		if err != nil {
			return err
		}
		debits, err := money.FromMinor(totals.DebitsMinor, w.Currency())
		if err != nil {
			return err
		}
		calculated, err := credits.Sub(debits)
		if err != nil {
			return err
		}
		diff, err := w.Balance().Sub(calculated)
		if err != nil {
			return err
		}
		r = Reconciliation{
			WalletID: walletID, StoredBalance: w.Balance(), CalculatedBalance: calculated,
			Difference: diff, Consistent: diff.IsZero(), CheckedEntries: totals.Entries,
		}
		return nil
	})
	if err != nil {
		return Reconciliation{}, err
	}
	s.metrics.Reconciliation(r.Consistent)
	if !r.Consistent {
		s.log.Error("divergência de reconciliação",
			slog.String("walletId", walletID.String()), slog.String("correlationId", correlationID),
			slog.String("difference", r.Difference.Amount()), slog.Int64("checkedEntries", r.CheckedEntries))
	} else {
		s.log.Info("reconciliação consistente", slog.String("walletId", walletID.String()),
			slog.String("correlationId", correlationID), slog.Int64("checkedEntries", r.CheckedEntries))
	}
	return r, nil
}

// TransactionQueries consulta transações.
type TransactionQueries struct{ uow UnitOfWork }

// NewTransactionQueries cria o serviço de consultas.
func NewTransactionQueries(uow UnitOfWork) *TransactionQueries { return &TransactionQueries{uow: uow} }

// Get busca por ID interno.
func (q *TransactionQueries) Get(ctx context.Context, id uuid.UUID) (wager.Snapshot, error) {
	var snap wager.Snapshot
	err := q.uow.ReadSnapshot(ctx, func(ctx context.Context, st Store) error {
		tx, err := st.Transactions().Get(ctx, id)
		if err != nil {
			return err
		}
		snap = tx.Snapshot()
		return nil
	})
	return snap, err
}

// GetByExternalID busca por (providerId, externalTransactionId).
func (q *TransactionQueries) GetByExternalID(ctx context.Context, providerID, externalID string) (wager.Snapshot, error) {
	var snap wager.Snapshot
	err := q.uow.ReadSnapshot(ctx, func(ctx context.Context, st Store) error {
		tx, err := st.Transactions().FindByExternalID(ctx, providerID, externalID)
		if err != nil {
			return err
		}
		if tx == nil {
			return ErrNotFound
		}
		snap = tx.Snapshot()
		return nil
	})
	if errors.Is(err, ErrNotFound) {
		return wager.Snapshot{}, ErrNotFound
	}
	return snap, err
}
