package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vinipenido/backend-challenge-go/internal/app"
)

// querier é o subconjunto comum a pgx.Tx e *pgxpool.Pool.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// UnitOfWork delimita a transação SQL: todos os repositórios recebem a mesma
// pgx.Tx, e o COMMIT só ocorre se a função devolver nil.
type UnitOfWork struct {
	pool *pgxpool.Pool
}

// NewUnitOfWork cria a unidade de trabalho.
func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork { return &UnitOfWork{pool: pool} }

// Do executa fn em READ COMMITTED. A coordenação entre escritores é feita por
// locks explícitos de linha (SELECT ... FOR UPDATE na carteira), atualizações
// condicionadas à versão e constraints; lock_timeout (configurado na conexão)
// evita esperas indefinidas.
func (u *UnitOfWork) Do(ctx context.Context, fn func(ctx context.Context, st app.Store) error) error {
	return u.run(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, fn)
}

// ReadSnapshot executa fn em REPEATABLE READ READ ONLY: todas as consultas
// enxergam o mesmo snapshot (usado na reconciliação e nas leituras).
func (u *UnitOfWork) ReadSnapshot(ctx context.Context, fn func(ctx context.Context, st app.Store) error) error {
	return u.run(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, fn)
}

func (u *UnitOfWork) run(ctx context.Context, opts pgx.TxOptions, fn func(ctx context.Context, st app.Store) error) error {
	tx, err := u.pool.BeginTx(ctx, opts)
	if err != nil {
		return classify(err)
	}
	committed := false
	defer func() {
		if !committed {
			// Rollback com contexto próprio: o contexto da requisição pode já ter expirado.
			rctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tx.Rollback(rctx)
		}
	}()
	if err := fn(ctx, &store{q: tx}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		committed = true // a conexão já foi descartada pelo driver
		return classify(err)
	}
	committed = true
	return nil
}

type store struct{ q querier }

func (s *store) Wallets() app.WalletRepository           { return &walletRepo{q: s.q} }
func (s *store) Transactions() app.TransactionRepository { return &txRepo{q: s.q} }
func (s *store) Ledger() app.LedgerRepository            { return &ledgerRepo{q: s.q} }
func (s *store) Outbox() app.OutboxWriter                { return &outboxWriter{q: s.q} }
func (s *store) Inbox() app.InboxRepository              { return &inboxRepo{q: s.q} }
