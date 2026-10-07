// Package wallet contém a raiz do agregado financeiro (Wallet) e o ledger.
package wallet

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/vinipenido/backend-challenge-go/internal/domain/event"
	"github.com/vinipenido/backend-challenge-go/internal/domain/money"
)

// Erros de domínio da carteira, classificáveis via errors.Is.
var (
	ErrInvalidWallet      = errors.New("wallet: dados da carteira inválidos")
	ErrInvalidAmount      = errors.New("wallet: valor da movimentação deve ser positivo")
	ErrCurrencyMismatch   = errors.New("wallet: moeda da movimentação difere da moeda da carteira")
	ErrInsufficientFunds  = errors.New("wallet: saldo insuficiente")
	ErrInvalidLedgerEntry = errors.New("wallet: lançamento de ledger inválido")
)

// InitialVersion é a versão de toda carteira recém-criada.
const InitialVersion int64 = 1

// Wallet é a raiz do agregado. O saldo só muda por Debit/Credit, que geram o
// lançamento correspondente e incrementam a versão.
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	currency  money.Currency
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time

	events []event.Envelope
}

// OpenParams descreve a abertura de uma carteira.
type OpenParams struct {
	ID             uuid.UUID
	PlayerID       uuid.UUID
	InitialBalance money.Money
	// Identidades usadas somente quando o saldo inicial é positivo.
	OpeningTransactionID uuid.UUID
	OpeningEntryID       uuid.UUID
	Meta                 event.Metadata
}

// Open cria uma carteira na versão 1. Saldo inicial positivo gera o lançamento
// de crédito de abertura (0 -> saldo inicial) e o evento WalletBalanceChanged;
// saldo zero não gera lançamento nem evento.
func Open(p OpenParams) (*Wallet, *LedgerEntry, error) {
	if p.ID == uuid.Nil || p.PlayerID == uuid.Nil {
		return nil, nil, fmt.Errorf("%w: id e playerId são obrigatórios", ErrInvalidWallet)
	}
	if err := p.InitialBalance.Validate(); err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrInvalidWallet, err)
	}
	if p.InitialBalance.IsNegative() {
		return nil, nil, fmt.Errorf("%w: saldo inicial negativo", ErrInvalidAmount)
	}
	if p.Meta.OccurredAt.IsZero() {
		return nil, nil, fmt.Errorf("%w: instante de criação obrigatório", ErrInvalidWallet)
	}
	now := p.Meta.OccurredAt.UTC()
	w := &Wallet{
		id:        p.ID,
		playerID:  p.PlayerID,
		currency:  p.InitialBalance.Currency(),
		balance:   p.InitialBalance,
		version:   InitialVersion,
		createdAt: now,
		updatedAt: now,
	}
	if p.InitialBalance.IsZero() {
		return w, nil, nil
	}
	zero, _ := money.Zero(w.currency)
	entry, err := NewLedgerEntry(LedgerEntryParams{
		ID:            p.OpeningEntryID,
		WalletID:      w.id,
		TransactionID: p.OpeningTransactionID,
		Direction:     Credit,
		Amount:        p.InitialBalance,
		BalanceBefore: zero,
		BalanceAfter:  p.InitialBalance,
		WalletVersion: w.version,
		CreatedAt:     now,
	})
	if err != nil {
		return nil, nil, err
	}
	w.recordBalanceChanged(entry, p.Meta)
	return w, &entry, nil
}

// RehydrateParams contém o estado persistido de uma carteira.
type RehydrateParams struct {
	ID        uuid.UUID
	PlayerID  uuid.UUID
	Balance   money.Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Rehydrate reconstrói a carteira a partir do banco, sem reaplicar
// movimentações nem emitir eventos.
func Rehydrate(p RehydrateParams) (*Wallet, error) {
	if p.ID == uuid.Nil || p.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: identificadores obrigatórios", ErrInvalidWallet)
	}
	if err := p.Balance.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidWallet, err)
	}
	if p.Balance.IsNegative() {
		return nil, fmt.Errorf("%w: saldo persistido negativo", ErrInvalidWallet)
	}
	if p.Version < InitialVersion {
		return nil, fmt.Errorf("%w: versão %d inválida", ErrInvalidWallet, p.Version)
	}
	if p.CreatedAt.IsZero() || p.UpdatedAt.IsZero() {
		return nil, fmt.Errorf("%w: timestamps obrigatórios", ErrInvalidWallet)
	}
	return &Wallet{
		id:        p.ID,
		playerID:  p.PlayerID,
		currency:  p.Balance.Currency(),
		balance:   p.Balance,
		version:   p.Version,
		createdAt: p.CreatedAt.UTC(),
		updatedAt: p.UpdatedAt.UTC(),
	}, nil
}

// Movement descreve uma movimentação financeira solicitada à carteira.
type Movement struct {
	TransactionID uuid.UUID
	EntryID       uuid.UUID
	Amount        money.Money
	Meta          event.Metadata
}

// Debit retira o valor do saldo, preservando saldo >= 0.
func (w *Wallet) Debit(m Movement) (LedgerEntry, error) {
	return w.apply(Debit, m)
}

// Credit adiciona o valor ao saldo.
func (w *Wallet) Credit(m Movement) (LedgerEntry, error) {
	return w.apply(Credit, m)
}

func (w *Wallet) apply(dir Direction, m Movement) (LedgerEntry, error) {
	if err := m.Amount.Validate(); err != nil {
		return LedgerEntry{}, fmt.Errorf("%w: %v", ErrInvalidAmount, err)
	}
	if m.Amount.Currency() != w.currency {
		return LedgerEntry{}, fmt.Errorf("%w: %s != %s", ErrCurrencyMismatch, m.Amount.Currency(), w.currency)
	}
	if !m.Amount.IsPositive() {
		return LedgerEntry{}, ErrInvalidAmount
	}
	if m.Meta.OccurredAt.IsZero() {
		return LedgerEntry{}, fmt.Errorf("%w: instante obrigatório", ErrInvalidWallet)
	}
	var after money.Money
	var err error
	if dir == Debit {
		after, err = w.balance.Sub(m.Amount)
	} else {
		after, err = w.balance.Add(m.Amount)
	}
	if err != nil {
		return LedgerEntry{}, err
	}
	if after.IsNegative() {
		return LedgerEntry{}, ErrInsufficientFunds
	}
	now := m.Meta.OccurredAt.UTC()
	entry, err := NewLedgerEntry(LedgerEntryParams{
		ID:            m.EntryID,
		WalletID:      w.id,
		TransactionID: m.TransactionID,
		Direction:     dir,
		Amount:        m.Amount,
		BalanceBefore: w.balance,
		BalanceAfter:  after,
		WalletVersion: w.version + 1,
		CreatedAt:     now,
	})
	if err != nil {
		return LedgerEntry{}, err
	}
	// Só altera o estado depois que o lançamento foi validado.
	w.balance = after
	w.version++
	w.updatedAt = now
	w.recordBalanceChanged(entry, m.Meta)
	return entry, nil
}

func (w *Wallet) recordBalanceChanged(entry LedgerEntry, meta event.Metadata) {
	// A causa do evento de saldo é a transação que o originou.
	meta.CausationID = entry.TransactionID().String()
	w.events = append(w.events, event.NewWalletBalanceChanged(w.id, meta, event.WalletBalanceChangedData{
		WalletID:      w.id.String(),
		TransactionID: entry.TransactionID().String(),
		Direction:     string(entry.Direction()),
		Money:         entry.Amount(),
		BalanceBefore: entry.BalanceBefore(),
		BalanceAfter:  entry.BalanceAfter(),
		WalletVersion: entry.WalletVersion(),
	}))
}

// PullEvents devolve e remove os eventos pendentes gerados pelo agregado.
func (w *Wallet) PullEvents() []event.Envelope {
	out := w.events
	w.events = nil
	return out
}

// Métodos de acesso (somente leitura).

func (w *Wallet) ID() uuid.UUID            { return w.id }
func (w *Wallet) PlayerID() uuid.UUID      { return w.playerID }
func (w *Wallet) Currency() money.Currency { return w.currency }
func (w *Wallet) Balance() money.Money     { return w.balance }
func (w *Wallet) Version() int64           { return w.version }
func (w *Wallet) CreatedAt() time.Time     { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time     { return w.updatedAt }
