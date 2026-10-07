// Package app contém os casos de uso e as portas (interfaces) que os adaptadores
// de persistência, mensageria e HTTP implementam ou consomem.
package app

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/vinipenido/backend-challenge-go/internal/domain/event"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wager"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wallet"
)

// UnitOfWork delimita a transação SQL. Todos os repositórios obtidos de Store
// dentro de fn compartilham a mesma transação; o COMMIT acontece somente se fn
// devolver nil.
type UnitOfWork interface {
	// Do executa fn em READ COMMITTED com locks explícitos de linha.
	Do(ctx context.Context, fn func(ctx context.Context, st Store) error) error
	// ReadSnapshot executa fn em REPEATABLE READ READ ONLY (visão consistente).
	ReadSnapshot(ctx context.Context, fn func(ctx context.Context, st Store) error) error
}

// Store agrupa os repositórios ligados a uma mesma transação.
type Store interface {
	Wallets() WalletRepository
	Transactions() TransactionRepository
	Ledger() LedgerRepository
	Outbox() OutboxWriter
	Inbox() InboxRepository
}

// WalletRepository persiste o agregado Wallet.
type WalletRepository interface {
	Insert(ctx context.Context, w *wallet.Wallet) error
	Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// GetForUpdate obtém a carteira com lock exclusivo de linha (SELECT ... FOR UPDATE).
	GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// UpdateBalance grava saldo/versão condicionado à versão esperada
	// (UPDATE ... WHERE version = expected), devolvendo ErrConcurrencyConflict se divergir.
	UpdateBalance(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error
}

// PendingRef identifica uma transação pendente a ser retomada.
type PendingRef struct {
	ID       uuid.UUID
	WalletID uuid.UUID
}

// TransactionRepository persiste WagerTransaction.
type TransactionRepository interface {
	Insert(ctx context.Context, s wager.Snapshot) error
	// Update grava o novo estado condicionado ao estado anterior esperado.
	Update(ctx context.Context, s wager.Snapshot, expectedStatus wager.Status) error
	Get(ctx context.Context, id uuid.UUID) (*wager.Transaction, error)
	// FindIdempotencyCandidates busca, em uma única consulta (mesmo snapshot),
	// as transações do provedor com a mesma chave OU o mesmo ID externo.
	FindIdempotencyCandidates(ctx context.Context, providerID, key, externalID string) ([]*wager.Transaction, error)
	FindByExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
	HasProcessedReversal(ctx context.Context, referenceID uuid.UUID) (bool, error)
	ListDuePending(ctx context.Context, now time.Time, limit int) ([]PendingRef, error)
	// LockDuePending trava (FOR UPDATE SKIP LOCKED) uma pendência vencida;
	// devolve nil se já foi tratada ou está em uso por outra instância.
	LockDuePending(ctx context.Context, id uuid.UUID, now time.Time) (*wager.Transaction, error)
	// WakeDependents antecipa a próxima tentativa das operações que aguardam
	// a referência (providerID, externalID).
	WakeDependents(ctx context.Context, providerID, externalID string, now time.Time) (int64, error)
}

// LedgerTotals resume o ledger de uma carteira.
type LedgerTotals struct {
	CreditsMinor int64
	DebitsMinor  int64
	Entries      int64
}

// LedgerRepository persiste lançamentos (somente inserção e leitura).
type LedgerRepository interface {
	Insert(ctx context.Context, e wallet.LedgerEntry) error
	List(ctx context.Context, walletID uuid.UUID, afterVersion int64, limit int) ([]wallet.LedgerEntry, error)
	Totals(ctx context.Context, walletID uuid.UUID) (LedgerTotals, error)
}

// OutboxWriter grava eventos na outbox dentro da transação corrente.
type OutboxWriter interface {
	Append(ctx context.Context, events ...event.Envelope) error
}

// InboxMessage é o registro de deduplicação de uma mensagem recebida.
type InboxMessage struct {
	Consumer      string
	MessageID     string
	Hash          string
	TransactionID *uuid.UUID
	ReceivedAt    time.Time
	ProcessedAt   time.Time
}

// InboxRepository persiste a inbox.
type InboxRepository interface {
	Get(ctx context.Context, consumer, messageID string) (*InboxMessage, error)
	// Record insere a mensagem como concluída; devolve ErrConcurrencyConflict se
	// outra transação registrou a mesma mensagem em paralelo.
	Record(ctx context.Context, m InboxMessage) error
}

// Clock abstrai o relógio (testável).
type Clock interface{ Now() time.Time }

// SystemClock usa time.Now em UTC.
type SystemClock struct{}

// Now devolve o instante atual em UTC.
func (SystemClock) Now() time.Time { return time.Now().UTC() }

// IDGenerator gera identificadores (UUIDv7, ordenáveis no tempo).
type IDGenerator interface{ New() uuid.UUID }

// UUIDv7 é o gerador padrão.
type UUIDv7 struct{}

// New devolve um UUIDv7.
func (UUIDv7) New() uuid.UUID { return uuid.Must(uuid.NewV7()) }

// Metrics é a porta de métricas usada pelos casos de uso.
type Metrics interface {
	TransactionOutcome(channel, kind, status string, replay bool, d time.Duration)
	Duplicate(channel, reason string)
	IdempotencyConflict(channel string)
	ConcurrencyConflict(reason string)
	Retry(component string)
	Reconciliation(consistent bool)
}

// NopMetrics descarta as métricas (testes).
type NopMetrics struct{}

func (NopMetrics) TransactionOutcome(string, string, string, bool, time.Duration) {}
func (NopMetrics) Duplicate(string, string)                                       {}
func (NopMetrics) IdempotencyConflict(string)                                     {}
func (NopMetrics) ConcurrencyConflict(string)                                     {}
func (NopMetrics) Retry(string)                                                   {}
func (NopMetrics) Reconciliation(bool)                                            {}
