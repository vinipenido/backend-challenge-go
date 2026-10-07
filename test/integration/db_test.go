//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func pool(t *testing.T, url string) *pgxpool.Pool {
	t.Helper()
	p, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func pgCode(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// fixture cria, como dono do schema, uma carteira com abertura de 100.00 e um BET de 30.00.
type fixture struct {
	wallet, player, opening, bet, betEntry uuid.UUID
}

func newFixture(t *testing.T, owner *pgxpool.Pool) fixture {
	t.Helper()
	f := fixture{wallet: uuid.New(), player: uuid.New(), opening: uuid.New(), bet: uuid.New(), betEntry: uuid.New()}
	now := time.Now().UTC()
	ext := "fx-" + uuid.NewString()
	err := pgx.BeginFunc(context.Background(), owner, func(tx pgx.Tx) error {
		stmts := []struct {
			sql  string
			args []any
		}{
			{`INSERT INTO wallets VALUES ($1,$2,'BRL',7000,2,$3,$3)`, []any{f.wallet, f.player, now}},
			{`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency, result_balance_minor, created_at, updated_at, completed_at)
			  VALUES ($1,'INTERNAL','OPENING','PROCESSED',$2,$3,10000,'BRL',10000,$4,$4,$4)`, []any{f.opening, f.wallet, f.player, now}},
			{`INSERT INTO wallet_ledger_entries VALUES ($1,$2,$3,'CREDIT',10000,'BRL',0,10000,1,$4)`, []any{uuid.New(), f.wallet, f.opening, now}},
			{`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency, provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id, result_balance_minor, created_at, updated_at, completed_at)
			  VALUES ($1,'EXTERNAL','BET','PROCESSED',$2,$3,3000,'BRL','provider-a',$4,$4,repeat('a',64),'r','g',7000,$5,$5,$5)`, []any{f.bet, f.wallet, f.player, ext, now}},
			{`INSERT INTO wallet_ledger_entries VALUES ($1,$2,$3,'DEBIT',3000,'BRL',10000,7000,2,$4)`, []any{f.betEntry, f.wallet, f.bet, now}},
		}
		for _, s := range stmts {
			if _, err := tx.Exec(context.Background(), s.sql, s.args...); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	return f
}

func expectErr(t *testing.T, name string, err error, codes ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: esperado erro, operação foi aceita", name)
	}
	got := pgCode(err)
	for _, c := range codes {
		if got == c {
			return
		}
	}
	t.Fatalf("%s: código %q (%v), esperado um de %v", name, got, err, codes)
}

func TestMigrationsAppliedAndReversible(t *testing.T) {
	// O TestMain executou down-all e up; aqui conferimos versão e objetos essenciais.
	if v := queryInt(t, `SELECT version FROM schema_migrations`); v != 1 {
		t.Fatalf("versão = %d", v)
	}
	for _, obj := range []string{"wallets", "wager_transactions", "wallet_ledger_entries", "outbox_events", "inbox_messages"} {
		if queryInt(t, `SELECT COUNT(*) FROM information_schema.tables WHERE table_name = $1`, obj) != 1 {
			t.Fatalf("tabela %s ausente", obj)
		}
	}
	for _, idx := range []string{"wager_tx_provider_idempotency_uk", "wager_tx_provider_external_uk", "wager_tx_single_opening_uk", "wager_tx_single_reversal_uk"} {
		if queryInt(t, `SELECT COUNT(*) FROM pg_indexes WHERE indexname = $1`, idx) != 1 {
			t.Fatalf("índice %s ausente", idx)
		}
	}
}

func TestDatabaseInvariants(t *testing.T) {
	ctx := context.Background()
	owner := pool(t, ownerURL)
	app := pool(t, appURL(pgHost))
	f := newFixture(t, owner)

	t.Run("ledger imutável para o papel da aplicação (privilégios)", func(t *testing.T) {
		_, err := app.Exec(ctx, `UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE id = $1`, f.betEntry)
		expectErr(t, "UPDATE", err, "42501")
		_, err = app.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE id = $1`, f.betEntry)
		expectErr(t, "DELETE", err, "42501")
		_, err = app.Exec(ctx, `TRUNCATE wallet_ledger_entries`)
		expectErr(t, "TRUNCATE", err, "42501")
	})

	t.Run("ledger imutável mesmo para o dono (triggers)", func(t *testing.T) {
		_, err := owner.Exec(ctx, `UPDATE wallet_ledger_entries SET created_at = now() WHERE id = $1`, f.betEntry)
		expectErr(t, "UPDATE", err, "23000")
		_, err = owner.Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE id = $1`, f.betEntry)
		expectErr(t, "DELETE", err, "23000")
		_, err = owner.Exec(ctx, `TRUNCATE wallet_ledger_entries CASCADE`)
		expectErr(t, "TRUNCATE", err, "23000")
	})

	t.Run("saldo negativo é impossível", func(t *testing.T) {
		_, err := app.Exec(ctx, `UPDATE wallets SET balance_minor = -1, version = version + 1 WHERE id = $1`, f.wallet)
		expectErr(t, "saldo negativo", err, "23514")
	})

	t.Run("saldo sem lançamento correspondente falha no COMMIT", func(t *testing.T) {
		err := pgx.BeginFunc(ctx, app, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `UPDATE wallets SET balance_minor = 9000, version = version + 1 WHERE id = $1`, f.wallet)
			return err
		})
		expectErr(t, "sem ledger", err, "23000")
		if walletBalance(t, f.wallet.String()) != 7000 {
			t.Fatal("saldo não pode ter mudado")
		}
	})

	t.Run("versão só muda junto com o saldo, de 1 em 1", func(t *testing.T) {
		_, err := app.Exec(ctx, `UPDATE wallets SET version = version + 1 WHERE id = $1`, f.wallet)
		expectErr(t, "versão sem saldo", err, "23000")
		_, err = app.Exec(ctx, `UPDATE wallets SET balance_minor = 6000, version = version + 5 WHERE id = $1`, f.wallet)
		expectErr(t, "salto de versão", err, "23000")
	})

	t.Run("conta do lançamento e encadeamento", func(t *testing.T) {
		_, err := app.Exec(ctx, `INSERT INTO wallet_ledger_entries VALUES ($1,$2,$3,'DEBIT',100,'BRL',7000,7100,3,now())`, uuid.New(), f.wallet, f.opening)
		expectErr(t, "conta errada", err, "23514")
		_, err = app.Exec(ctx, `INSERT INTO wallet_ledger_entries VALUES ($1,$2,$3,'DEBIT',100,'BRL',5000,4900,3,now())`, uuid.New(), f.wallet, f.opening)
		expectErr(t, "encadeamento", err, "23514")
	})

	t.Run("unicidade (walletId, transactionId) e versão", func(t *testing.T) {
		_, err := owner.Exec(ctx, `INSERT INTO wallet_ledger_entries VALUES ($1,$2,$3,'DEBIT',100,'BRL',7000,6900,3,now())`, uuid.New(), f.wallet, f.bet)
		expectErr(t, "mesmo transactionId", err, "23505")
	})

	t.Run("crédito inicial duplicado", func(t *testing.T) {
		_, err := owner.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency, result_balance_minor, created_at, updated_at, completed_at)
			VALUES ($1,'INTERNAL','OPENING','PROCESSED',$2,$3,1,'BRL',1,now(),now(),now())`, uuid.New(), f.wallet, f.player)
		expectErr(t, "segunda abertura", err, "23505")
	})

	t.Run("origem interna sem metadados externos", func(t *testing.T) {
		_, err := owner.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency, provider_id, created_at, updated_at)
			VALUES ($1,'INTERNAL','OPENING','PENDING',$2,$3,1,'BRL','provider-a',now(),now())`, uuid.New(), f.wallet, f.player)
		expectErr(t, "OPENING com provedor", err, "23514")
	})

	t.Run("transação terminal é imutável", func(t *testing.T) {
		_, err := app.Exec(ctx, `UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'INSUFFICIENT_FUNDS' WHERE id = $1`, f.bet)
		expectErr(t, "terminal", err, "23000")
		_, err = app.Exec(ctx, `DELETE FROM wager_transactions WHERE id = $1`, f.bet)
		expectErr(t, "delete", err, "42501")
	})

	t.Run("uma única reversão bem-sucedida por referência", func(t *testing.T) {
		insert := func(kind string) error {
			_, err := owner.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency, provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id, reference_external_transaction_id, reference_transaction_id, result_balance_minor, created_at, updated_at, completed_at)
				VALUES ($1,'EXTERNAL',$2,'PROCESSED',$3,$4,3000,'BRL','provider-a',$5,$5,repeat('b',64),'r','g','x',$6,10000,now(),now(),now())`,
				uuid.New(), kind, f.wallet, f.player, "rv-"+uuid.NewString(), f.bet)
			return err
		}
		if err := insert("REFUND"); err != nil {
			t.Fatal(err)
		}
		expectErr(t, "segunda reversão", insert("ROLLBACK"), "23505")
	})

	t.Run("snapshot da outbox é imutável", func(t *testing.T) {
		id := uuid.New()
		if _, err := app.Exec(ctx, `INSERT INTO outbox_events (event_id, aggregate_type, aggregate_id, event_type, event_version, partition_key, payload, occurred_at)
			VALUES ($1,'Wallet',$2,'WalletBalanceChanged',1,$3,'{"a":1}',now())`, id, f.wallet, f.wallet.String()); err != nil {
			t.Fatal(err)
		}
		_, err := app.Exec(ctx, `UPDATE outbox_events SET payload = '{"a":2}' WHERE event_id = $1`, id)
		expectErr(t, "payload", err, "23000")
		if _, err := app.Exec(ctx, `UPDATE outbox_events SET attempts = attempts + 1 WHERE event_id = $1`, id); err != nil {
			t.Fatalf("campos de controle devem ser atualizáveis: %v", err)
		}
		_, _ = owner.Exec(ctx, `UPDATE outbox_events SET published_at = now() WHERE event_id = $1`, id)
	})

	t.Run("unicidade da inbox por (consumer, messageId)", func(t *testing.T) {
		msg := "m-" + uuid.NewString()
		ins := func() error {
			_, err := app.Exec(ctx, `INSERT INTO inbox_messages VALUES ('c', $1, repeat('a',64), NULL, now(), now())`, msg)
			return err
		}
		if err := ins(); err != nil {
			t.Fatal(err)
		}
		expectErr(t, "inbox duplicada", ins(), "23505")
	})
}
