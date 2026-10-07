package wallet_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vinipenido/backend-challenge-go/internal/domain/event"
	"github.com/vinipenido/backend-challenge-go/internal/domain/money"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wallet"
)

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.Parse(s, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func meta() event.Metadata { return event.Metadata{CorrelationID: "corr-1", OccurredAt: now} }

func openWallet(t *testing.T, initial string) (*wallet.Wallet, *wallet.LedgerEntry) {
	t.Helper()
	w, entry, err := wallet.Open(wallet.OpenParams{
		ID: uuid.New(), PlayerID: uuid.New(), InitialBalance: brl(t, initial),
		OpeningTransactionID: uuid.New(), OpeningEntryID: uuid.New(), Meta: meta(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return w, entry
}

func TestOpenWithPositiveBalance(t *testing.T) {
	w, entry := openWallet(t, "1000.00")
	if w.Version() != 1 {
		t.Fatalf("versão inicial = %d, esperado 1", w.Version())
	}
	if w.Balance().Amount() != "1000.00" {
		t.Fatalf("saldo = %s", w.Balance())
	}
	if entry == nil {
		t.Fatal("abertura com saldo positivo deve gerar lançamento")
	}
	if entry.Direction() != wallet.Credit || entry.BalanceBefore().Amount() != "0.00" ||
		entry.BalanceAfter().Amount() != "1000.00" || entry.WalletVersion() != 1 {
		t.Fatalf("lançamento de abertura incorreto: %+v", entry)
	}
	evs := w.PullEvents()
	if len(evs) != 1 || evs[0].EventType != event.TypeWalletBalanceChanged {
		t.Fatalf("eventos = %+v", evs)
	}
	data := evs[0].Data.(event.WalletBalanceChangedData)
	if data.WalletVersion != 1 || data.Direction != "CREDIT" {
		t.Fatalf("payload = %+v", data)
	}
	if len(w.PullEvents()) != 0 {
		t.Fatal("PullEvents deve esvaziar a lista")
	}
}

func TestOpenWithZeroBalance(t *testing.T) {
	w, entry := openWallet(t, "0.00")
	if entry != nil {
		t.Fatal("saldo zero não deve gerar lançamento")
	}
	if len(w.PullEvents()) != 0 {
		t.Fatal("saldo zero não deve gerar eventos")
	}
	if w.Version() != 1 {
		t.Fatalf("versão = %d", w.Version())
	}
}

func TestOpenRejectsInvalid(t *testing.T) {
	neg := brl(t, "-1.00")
	if _, _, err := wallet.Open(wallet.OpenParams{ID: uuid.New(), PlayerID: uuid.New(), InitialBalance: neg, Meta: meta()}); !errors.Is(err, wallet.ErrInvalidAmount) {
		t.Fatalf("saldo negativo: %v", err)
	}
	if _, _, err := wallet.Open(wallet.OpenParams{ID: uuid.New(), PlayerID: uuid.New(), Meta: meta()}); !errors.Is(err, wallet.ErrInvalidWallet) {
		t.Fatalf("Money não inicializado: %v", err)
	}
	if _, _, err := wallet.Open(wallet.OpenParams{PlayerID: uuid.New(), InitialBalance: brl(t, "1.00"), Meta: meta()}); !errors.Is(err, wallet.ErrInvalidWallet) {
		t.Fatalf("sem id: %v", err)
	}
}

func move(t *testing.T, amount string) wallet.Movement {
	return wallet.Movement{TransactionID: uuid.New(), EntryID: uuid.New(), Amount: brl(t, amount), Meta: meta()}
}

func TestDebitAndCredit(t *testing.T) {
	w, _ := openWallet(t, "100.00")
	w.PullEvents()

	e1, err := w.Debit(move(t, "80.00"))
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatalf("após débito: %s v%d", w.Balance(), w.Version())
	}
	if e1.BalanceBefore().Amount() != "100.00" || e1.BalanceAfter().Amount() != "20.00" || e1.WalletVersion() != 2 {
		t.Fatalf("lançamento: %+v", e1)
	}

	// Saldo insuficiente: estado intacto.
	if _, err := w.Debit(move(t, "80.00")); !errors.Is(err, wallet.ErrInsufficientFunds) {
		t.Fatalf("esperado ErrInsufficientFunds, obtido %v", err)
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatalf("estado alterado após rejeição: %s v%d", w.Balance(), w.Version())
	}

	// Débito exato até zero é permitido.
	if _, err := w.Debit(move(t, "20.00")); err != nil {
		t.Fatal(err)
	}
	if !w.Balance().IsZero() || w.Version() != 3 {
		t.Fatalf("saldo = %s v%d", w.Balance(), w.Version())
	}

	if _, err := w.Credit(move(t, "5.50")); err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "5.50" || w.Version() != 4 {
		t.Fatalf("após crédito: %s v%d", w.Balance(), w.Version())
	}
	if n := len(w.PullEvents()); n != 3 {
		t.Fatalf("eventos = %d, esperado 3 (apenas mudanças efetivas)", n)
	}
}

func TestMovementValidation(t *testing.T) {
	w, _ := openWallet(t, "100.00")
	usd, _ := money.Parse("1.00", "USD")
	if _, err := w.Credit(wallet.Movement{TransactionID: uuid.New(), EntryID: uuid.New(), Amount: usd, Meta: meta()}); !errors.Is(err, wallet.ErrCurrencyMismatch) {
		t.Fatalf("moeda: %v", err)
	}
	if _, err := w.Debit(move(t, "0.00")); !errors.Is(err, wallet.ErrInvalidAmount) {
		t.Fatalf("zero: %v", err)
	}
	if _, err := w.Credit(wallet.Movement{TransactionID: uuid.New(), EntryID: uuid.New(), Meta: meta()}); !errors.Is(err, wallet.ErrInvalidAmount) {
		t.Fatalf("não inicializado: %v", err)
	}
	if w.Version() != 1 {
		t.Fatalf("versão alterada sem mudança de saldo: %d", w.Version())
	}
}

func TestRehydrateDoesNotEmitEvents(t *testing.T) {
	w, err := wallet.Rehydrate(wallet.RehydrateParams{
		ID: uuid.New(), PlayerID: uuid.New(), Balance: brl(t, "975.00"), Version: 2, CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(w.PullEvents()) != 0 || w.Version() != 2 || w.Balance().Amount() != "975.00" {
		t.Fatal("reidratação não deve reaplicar movimentações nem emitir eventos")
	}
	if _, err := wallet.Rehydrate(wallet.RehydrateParams{ID: uuid.New(), PlayerID: uuid.New(), Balance: brl(t, "-1.00"), Version: 1, CreatedAt: now, UpdatedAt: now}); !errors.Is(err, wallet.ErrInvalidWallet) {
		t.Fatalf("saldo negativo: %v", err)
	}
	if _, err := wallet.Rehydrate(wallet.RehydrateParams{ID: uuid.New(), PlayerID: uuid.New(), Balance: brl(t, "1.00"), Version: 0, CreatedAt: now, UpdatedAt: now}); !errors.Is(err, wallet.ErrInvalidWallet) {
		t.Fatalf("versão 0: %v", err)
	}
}

func TestLedgerEntryValidation(t *testing.T) {
	base := wallet.LedgerEntryParams{
		ID: uuid.New(), WalletID: uuid.New(), TransactionID: uuid.New(), Direction: wallet.Debit,
		Amount: brl(t, "10.00"), BalanceBefore: brl(t, "100.00"), BalanceAfter: brl(t, "90.00"),
		WalletVersion: 2, CreatedAt: now,
	}
	if _, err := wallet.NewLedgerEntry(base); err != nil {
		t.Fatal(err)
	}
	bad := base
	bad.BalanceAfter = brl(t, "110.00")
	if _, err := wallet.NewLedgerEntry(bad); !errors.Is(err, wallet.ErrInvalidLedgerEntry) {
		t.Fatalf("débito incoerente: %v", err)
	}
	bad = base
	bad.Direction = wallet.Credit
	if _, err := wallet.NewLedgerEntry(bad); !errors.Is(err, wallet.ErrInvalidLedgerEntry) {
		t.Fatalf("crédito incoerente: %v", err)
	}
	bad = base
	bad.Amount = brl(t, "0.00")
	bad.BalanceAfter = bad.BalanceBefore
	if _, err := wallet.NewLedgerEntry(bad); !errors.Is(err, wallet.ErrInvalidLedgerEntry) {
		t.Fatalf("valor zero: %v", err)
	}
	bad = base
	bad.Direction = "SIDEWAYS"
	if _, err := wallet.NewLedgerEntry(bad); !errors.Is(err, wallet.ErrInvalidLedgerEntry) {
		t.Fatalf("direção: %v", err)
	}
	bad = base
	bad.BalanceBefore = brl(t, "5.00")
	bad.BalanceAfter = brl(t, "-5.00")
	if _, err := wallet.NewLedgerEntry(bad); !errors.Is(err, wallet.ErrInvalidLedgerEntry) {
		t.Fatalf("saldo negativo: %v", err)
	}
}

func TestBalanceChangedEventContract(t *testing.T) {
	w, _ := openWallet(t, "100.00")
	w.PullEvents()
	if _, err := w.Debit(move(t, "25.00")); err != nil {
		t.Fatal(err)
	}
	ev := w.PullEvents()[0]
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	for _, want := range []string{
		`"eventType":"WalletBalanceChanged"`, `"version":1`, `"correlationId":"corr-1"`,
		`"occurredAt":"2026-09-08T12:00:00.000Z"`, `"direction":"DEBIT"`,
		`"money":{"amount":"25.00","currency":"BRL"}`, `"balanceBefore":{"amount":"100.00","currency":"BRL"}`,
		`"balanceAfter":{"amount":"75.00","currency":"BRL"}`, `"walletVersion":2`, `"causationId":"`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("evento sem %s: %s", want, s)
		}
	}
}
