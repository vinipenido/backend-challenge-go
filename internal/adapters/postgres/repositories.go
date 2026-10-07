package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/vinipenido/backend-challenge-go/internal/app"
	"github.com/vinipenido/backend-challenge-go/internal/domain/event"
	"github.com/vinipenido/backend-challenge-go/internal/domain/money"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wager"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wallet"
)

// ---------------------------------------------------------------------------
// Carteiras
// ---------------------------------------------------------------------------

type walletRepo struct{ q querier }

const walletColumns = `id, player_id, currency, balance_minor, version, created_at, updated_at`

func scanWallet(row pgx.Row) (*wallet.Wallet, error) {
	var (
		id, playerID         uuid.UUID
		currency             string
		balance, version     int64
		createdAt, updatedAt time.Time
	)
	if err := row.Scan(&id, &playerID, &currency, &balance, &version, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, app.ErrWalletNotFound
		}
		return nil, classify(err)
	}
	c, err := money.ParseCurrency(currency)
	if err != nil {
		return nil, err
	}
	bal, err := money.FromMinor(balance, c)
	if err != nil {
		return nil, err
	}
	return wallet.Rehydrate(wallet.RehydrateParams{
		ID: id, PlayerID: playerID, Balance: bal, Version: version, CreatedAt: createdAt, UpdatedAt: updatedAt,
	})
}

func (r *walletRepo) Insert(ctx context.Context, w *wallet.Wallet) error {
	_, err := r.q.Exec(ctx, `
		INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), w.Currency().Code(), w.Balance().Minor(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	return classify(err)
}

func (r *walletRepo) Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return scanWallet(r.q.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id))
}

func (r *walletRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return scanWallet(r.q.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE`, id))
}

// UpdateBalance aplica controle otimista adicional ao lock pessimista: a linha
// só é alterada se a versão ainda for a esperada (nunca há lost update).
func (r *walletRepo) UpdateBalance(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error {
	tag, err := r.q.Exec(ctx, `
		UPDATE wallets SET balance_minor = $2, version = $3, updated_at = $4
		 WHERE id = $1 AND version = $5`,
		w.ID(), w.Balance().Minor(), w.Version(), w.UpdatedAt(), expectedVersion)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ConcurrencyConflict("version_mismatch", fmt.Errorf("carteira %s não está na versão %d", w.ID(), expectedVersion))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Transações
// ---------------------------------------------------------------------------

type txRepo struct{ q querier }

const txColumns = `id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
	provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
	reference_external_transaction_id, reference_transaction_id, failure_code, result_balance_minor,
	correlation_id, causation_id, attempts, next_attempt_at, expires_at, created_at, updated_at, completed_at`

func scanTx(row pgx.Row) (*wager.Transaction, error) {
	var (
		s                                                     wager.Snapshot
		origin, kind, status, currency                        string
		amount                                                int64
		providerID, extID, key, hash, roundID, gameID, refExt *string
		failure, correlation, causation                       *string
		resultBalance                                         *int64
	)
	err := row.Scan(&s.ID, &origin, &kind, &status, &s.WalletID, &s.PlayerID, &amount, &currency,
		&providerID, &extID, &key, &hash, &roundID, &gameID, &refExt, &s.ReferenceTransactionID, &failure,
		&resultBalance, &correlation, &causation, &s.Attempts, &s.NextAttemptAt, &s.ExpiresAt,
		&s.CreatedAt, &s.UpdatedAt, &s.CompletedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, app.ErrNotFound
		}
		return nil, classify(err)
	}
	c, err := money.ParseCurrency(currency)
	if err != nil {
		return nil, err
	}
	if s.Money, err = money.FromMinor(amount, c); err != nil {
		return nil, err
	}
	if resultBalance != nil {
		b, err := money.FromMinor(*resultBalance, c)
		if err != nil {
			return nil, err
		}
		s.ResultBalance = &b
	}
	s.Origin, s.Kind, s.Status = wager.Origin(origin), wager.Kind(kind), wager.Status(status)
	s.ProviderID, s.ExternalTransactionID, s.IdempotencyKey = deref(providerID), deref(extID), deref(key)
	s.PayloadHash, s.RoundID, s.GameID = deref(hash), deref(roundID), deref(gameID)
	s.ReferenceExternalTransactionID = deref(refExt)
	s.FailureCode = wager.FailureCode(deref(failure))
	s.CorrelationID, s.CausationID = deref(correlation), deref(causation)
	return wager.Rehydrate(s)
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func resultMinor(m *money.Money) *int64 {
	if m == nil {
		return nil
	}
	v := m.Minor()
	return &v
}

func (r *txRepo) Insert(ctx context.Context, s wager.Snapshot) error {
	_, err := r.q.Exec(ctx, `
		INSERT INTO wager_transactions (`+txColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)`,
		s.ID, string(s.Origin), string(s.Kind), string(s.Status), s.WalletID, s.PlayerID, s.Money.Minor(),
		s.Money.Currency().Code(), nullable(s.ProviderID), nullable(s.ExternalTransactionID),
		nullable(s.IdempotencyKey), nullable(s.PayloadHash), nullable(s.RoundID), nullable(s.GameID),
		nullable(s.ReferenceExternalTransactionID), s.ReferenceTransactionID, nullable(string(s.FailureCode)),
		resultMinor(s.ResultBalance), nullable(s.CorrelationID), nullable(s.CausationID), s.Attempts,
		s.NextAttemptAt, s.ExpiresAt, s.CreatedAt, s.UpdatedAt, s.CompletedAt)
	return classify(err)
}

func (r *txRepo) Update(ctx context.Context, s wager.Snapshot, expected wager.Status) error {
	tag, err := r.q.Exec(ctx, `
		UPDATE wager_transactions
		   SET status = $2, failure_code = $3, result_balance_minor = $4, reference_transaction_id = $5,
		       attempts = $6, next_attempt_at = $7, expires_at = $8, updated_at = $9, completed_at = $10
		 WHERE id = $1 AND status = $11`,
		s.ID, string(s.Status), nullable(string(s.FailureCode)), resultMinor(s.ResultBalance),
		s.ReferenceTransactionID, s.Attempts, s.NextAttemptAt, s.ExpiresAt, s.UpdatedAt, s.CompletedAt, string(expected))
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ConcurrencyConflict("status_mismatch", fmt.Errorf("transação %s não está em %s", s.ID, expected))
	}
	return nil
}

func (r *txRepo) Get(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	return scanTx(r.q.QueryRow(ctx, `SELECT `+txColumns+` FROM wager_transactions WHERE id = $1`, id))
}

func (r *txRepo) findOne(ctx context.Context, sql string, args ...any) (*wager.Transaction, error) {
	tx, err := scanTx(r.q.QueryRow(ctx, sql, args...))
	if errors.Is(err, app.ErrNotFound) {
		return nil, nil
	}
	return tx, err
}

func (r *txRepo) FindIdempotencyCandidates(ctx context.Context, providerID, key, externalID string) ([]*wager.Transaction, error) {
	rows, err := r.q.Query(ctx, `SELECT `+txColumns+` FROM wager_transactions
		WHERE origin = 'EXTERNAL' AND provider_id = $1 AND (idempotency_key = $2 OR external_transaction_id = $3)`,
		providerID, key, externalID)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []*wager.Transaction
	for rows.Next() {
		tx, err := scanTx(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, tx)
	}
	return out, classify(rows.Err())
}

func (r *txRepo) FindByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	return r.findOne(ctx, `SELECT `+txColumns+` FROM wager_transactions
		WHERE origin = 'EXTERNAL' AND provider_id = $1 AND external_transaction_id = $2`, providerID, externalID)
}

func (r *txRepo) HasProcessedReversal(ctx context.Context, referenceID uuid.UUID) (bool, error) {
	var exists bool
	err := r.q.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM wager_transactions
		 WHERE reference_transaction_id = $1 AND kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED')`,
		referenceID).Scan(&exists)
	return exists, classify(err)
}

func (r *txRepo) ListDuePending(ctx context.Context, now time.Time, limit int) ([]app.PendingRef, error) {
	rows, err := r.q.Query(ctx, `
		SELECT id, wallet_id FROM wager_transactions
		 WHERE status IN ('PENDING', 'PENDING_REFERENCE')
		   AND COALESCE(next_attempt_at, updated_at) <= $1
		 ORDER BY COALESCE(next_attempt_at, updated_at)
		 LIMIT $2`, now, limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []app.PendingRef
	for rows.Next() {
		var p app.PendingRef
		if err := rows.Scan(&p.ID, &p.WalletID); err != nil {
			return nil, classify(err)
		}
		out = append(out, p)
	}
	return out, classify(rows.Err())
}

func (r *txRepo) LockDuePending(ctx context.Context, id uuid.UUID, now time.Time) (*wager.Transaction, error) {
	return r.findOne(ctx, `SELECT `+txColumns+` FROM wager_transactions
		WHERE id = $1 AND status IN ('PENDING', 'PENDING_REFERENCE')
		  AND COALESCE(next_attempt_at, updated_at) <= $2
		FOR UPDATE SKIP LOCKED`, id, now)
}

func (r *txRepo) WakeDependents(ctx context.Context, providerID, externalID string, now time.Time) (int64, error) {
	tag, err := r.q.Exec(ctx, `
		UPDATE wager_transactions SET next_attempt_at = $3
		 WHERE status = 'PENDING_REFERENCE' AND provider_id = $1
		   AND reference_external_transaction_id = $2 AND next_attempt_at > $3`,
		providerID, externalID, now)
	if err != nil {
		return 0, classify(err)
	}
	return tag.RowsAffected(), nil
}

// ---------------------------------------------------------------------------
// Ledger
// ---------------------------------------------------------------------------

type ledgerRepo struct{ q querier }

func (r *ledgerRepo) Insert(ctx context.Context, e wallet.LedgerEntry) error {
	_, err := r.q.Exec(ctx, `
		INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency,
			balance_before_minor, balance_after_minor, wallet_version, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()), e.Amount().Minor(),
		e.Amount().Currency().Code(), e.BalanceBefore().Minor(), e.BalanceAfter().Minor(), e.WalletVersion(), e.CreatedAt())
	return classify(err)
}

func (r *ledgerRepo) List(ctx context.Context, walletID uuid.UUID, afterVersion int64, limit int) ([]wallet.LedgerEntry, error) {
	rows, err := r.q.Query(ctx, `
		SELECT id, wallet_id, transaction_id, direction, amount_minor, currency,
		       balance_before_minor, balance_after_minor, wallet_version, created_at
		  FROM wallet_ledger_entries
		 WHERE wallet_id = $1 AND wallet_version > $2
		 ORDER BY wallet_version
		 LIMIT $3`, walletID, afterVersion, limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []wallet.LedgerEntry
	for rows.Next() {
		var (
			p                     wallet.LedgerEntryParams
			direction, currency   string
			amount, before, after int64
		)
		if err := rows.Scan(&p.ID, &p.WalletID, &p.TransactionID, &direction, &amount, &currency,
			&before, &after, &p.WalletVersion, &p.CreatedAt); err != nil {
			return nil, classify(err)
		}
		c, err := money.ParseCurrency(currency)
		if err != nil {
			return nil, err
		}
		p.Direction = wallet.Direction(direction)
		p.Amount, _ = money.FromMinor(amount, c)
		p.BalanceBefore, _ = money.FromMinor(before, c)
		p.BalanceAfter, _ = money.FromMinor(after, c)
		e, err := wallet.NewLedgerEntry(p)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, classify(rows.Err())
}

func (r *ledgerRepo) Totals(ctx context.Context, walletID uuid.UUID) (app.LedgerTotals, error) {
	var t app.LedgerTotals
	err := r.q.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'CREDIT'), 0)::BIGINT,
		       COALESCE(SUM(amount_minor) FILTER (WHERE direction = 'DEBIT'), 0)::BIGINT,
		       COUNT(*)
		  FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).
		Scan(&t.CreditsMinor, &t.DebitsMinor, &t.Entries)
	return t, classify(err)
}

// ---------------------------------------------------------------------------
// Outbox (escrita transacional)
// ---------------------------------------------------------------------------

type outboxWriter struct{ q querier }

// Append serializa cada evento uma única vez; o JSON gravado é o snapshot
// imutável publicado (inclusive em republicações).
func (w *outboxWriter) Append(ctx context.Context, events ...event.Envelope) error {
	for _, ev := range events {
		payload, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		_, err = w.q.Exec(ctx, `
			INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, event_type, event_version,
				partition_key, correlation_id, payload, occurred_at, next_attempt_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)`,
			ev.EventID, ev.AggregateType, ev.AggregateID, string(ev.EventType), ev.Version,
			ev.PartitionKey, nullable(ev.CorrelationID), string(payload), ev.OccurredAt)
		if err != nil {
			return classify(err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Inbox
// ---------------------------------------------------------------------------

type inboxRepo struct{ q querier }

func (r *inboxRepo) Get(ctx context.Context, consumer, messageID string) (*app.InboxMessage, error) {
	var m app.InboxMessage
	err := r.q.QueryRow(ctx, `
		SELECT consumer_name, message_id, message_hash, transaction_id, received_at, processed_at
		  FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID).
		Scan(&m.Consumer, &m.MessageID, &m.Hash, &m.TransactionID, &m.ReceivedAt, &m.ProcessedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, classify(err)
	}
	return &m, nil
}

func (r *inboxRepo) Record(ctx context.Context, m app.InboxMessage) error {
	tag, err := r.q.Exec(ctx, `
		INSERT INTO inbox_messages (consumer_name, message_id, message_hash, transaction_id, received_at, processed_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (consumer_name, message_id) DO NOTHING`,
		m.Consumer, m.MessageID, m.Hash, m.TransactionID, m.ReceivedAt, m.ProcessedAt)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ConcurrencyConflict("inbox_duplicate", fmt.Errorf("mensagem %s registrada em paralelo", m.MessageID))
	}
	return nil
}
