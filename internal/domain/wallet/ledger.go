package wallet

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/vinipenido/backend-challenge-go/internal/domain/money"
)

// Direction é a direção de um lançamento no ledger.
type Direction string

// Direções possíveis.
const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

// ParseDirection valida uma direção persistida.
func ParseDirection(s string) (Direction, error) {
	switch Direction(s) {
	case Debit, Credit:
		return Direction(s), nil
	default:
		return "", fmt.Errorf("%w: direção %q", ErrInvalidLedgerEntry, s)
	}
}

// LedgerEntry é um lançamento imutável (append-only) do ledger da carteira.
// Todos os campos são privados e não há métodos de alteração.
type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	walletVersion int64
	createdAt     time.Time
}

// LedgerEntryParams agrupa os dados de construção/reidratação de um lançamento.
type LedgerEntryParams struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	WalletVersion int64
	CreatedAt     time.Time
}

// NewLedgerEntry valida balanceAfter = balanceBefore ± amount conforme a direção,
// além de valor positivo, saldos não negativos e moedas coerentes. O mesmo
// construtor é usado na reidratação, pois um lançamento não possui transições.
func NewLedgerEntry(p LedgerEntryParams) (LedgerEntry, error) {
	if p.ID == uuid.Nil || p.WalletID == uuid.Nil || p.TransactionID == uuid.Nil {
		return LedgerEntry{}, fmt.Errorf("%w: identificadores obrigatórios", ErrInvalidLedgerEntry)
	}
	if _, err := ParseDirection(string(p.Direction)); err != nil {
		return LedgerEntry{}, err
	}
	for _, m := range []money.Money{p.Amount, p.BalanceBefore, p.BalanceAfter} {
		if err := m.Validate(); err != nil {
			return LedgerEntry{}, fmt.Errorf("%w: %v", ErrInvalidLedgerEntry, err)
		}
	}
	if !p.Amount.IsPositive() {
		return LedgerEntry{}, fmt.Errorf("%w: valor deve ser positivo", ErrInvalidLedgerEntry)
	}
	if p.BalanceBefore.IsNegative() || p.BalanceAfter.IsNegative() {
		return LedgerEntry{}, fmt.Errorf("%w: saldos não podem ser negativos", ErrInvalidLedgerEntry)
	}
	if p.WalletVersion < 1 {
		return LedgerEntry{}, fmt.Errorf("%w: versão da carteira inválida", ErrInvalidLedgerEntry)
	}
	if p.CreatedAt.IsZero() {
		return LedgerEntry{}, fmt.Errorf("%w: instante de criação obrigatório", ErrInvalidLedgerEntry)
	}
	var expected money.Money
	var err error
	if p.Direction == Credit {
		expected, err = p.BalanceBefore.Add(p.Amount)
	} else {
		expected, err = p.BalanceBefore.Sub(p.Amount)
	}
	if err != nil {
		return LedgerEntry{}, fmt.Errorf("%w: %v", ErrInvalidLedgerEntry, err)
	}
	if !expected.Equal(p.BalanceAfter) {
		return LedgerEntry{}, fmt.Errorf("%w: balanceAfter %s difere de balanceBefore %s %s %s",
			ErrInvalidLedgerEntry, p.BalanceAfter, p.BalanceBefore, p.Direction, p.Amount)
	}
	return LedgerEntry{
		id: p.ID, walletID: p.WalletID, transactionID: p.TransactionID, direction: p.Direction,
		amount: p.Amount, balanceBefore: p.BalanceBefore, balanceAfter: p.BalanceAfter,
		walletVersion: p.WalletVersion, createdAt: p.CreatedAt.UTC(),
	}, nil
}

// Métodos de acesso (somente leitura).

func (e LedgerEntry) ID() uuid.UUID              { return e.id }
func (e LedgerEntry) WalletID() uuid.UUID        { return e.walletID }
func (e LedgerEntry) TransactionID() uuid.UUID   { return e.transactionID }
func (e LedgerEntry) Direction() Direction       { return e.direction }
func (e LedgerEntry) Amount() money.Money        { return e.amount }
func (e LedgerEntry) BalanceBefore() money.Money { return e.balanceBefore }
func (e LedgerEntry) BalanceAfter() money.Money  { return e.balanceAfter }
func (e LedgerEntry) WalletVersion() int64       { return e.walletVersion }
func (e LedgerEntry) CreatedAt() time.Time       { return e.createdAt }

// SignedAmount devolve +amount para crédito e -amount para débito.
func (e LedgerEntry) SignedAmount() (money.Money, error) {
	if e.direction == Debit {
		return e.amount.Neg()
	}
	return e.amount, nil
}
