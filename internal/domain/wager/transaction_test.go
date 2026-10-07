package wager_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vinipenido/backend-challenge-go/internal/domain/event"
	"github.com/vinipenido/backend-challenge-go/internal/domain/money"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wager"
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

var (
	walletID = uuid.New()
	playerID = uuid.New()
)

func params(t *testing.T, kind wager.Kind, amount, ref string) wager.ExternalParams {
	return wager.ExternalParams{
		ID: uuid.New(), ProviderID: "provider-a", ExternalTransactionID: "tx-" + uuid.NewString(),
		IdempotencyKey: "key-1", PayloadHash: strings.Repeat("a", 64), WalletID: walletID, PlayerID: playerID,
		RoundID: "round-1", GameID: "game-1", Kind: kind, Money: brl(t, amount),
		ReferenceExternalTransactionID: ref, CorrelationID: "corr", Now: now,
	}
}

func newTx(t *testing.T, kind wager.Kind, amount, ref string) *wager.Transaction {
	t.Helper()
	tx, err := wager.NewExternal(params(t, kind, amount, ref))
	if err != nil {
		t.Fatalf("NewExternal(%s,%s,%q): %v", kind, amount, ref, err)
	}
	return tx
}

func TestZeroValuePolicyPerKind(t *testing.T) {
	cases := []struct {
		kind   wager.Kind
		amount string
		ref    string
		err    error
	}{
		{wager.KindBet, "0.00", "", wager.ErrInvalidAmountForKind},
		{wager.KindBet, "0.01", "", nil},
		{wager.KindWin, "0.00", "", wager.ErrInvalidAmountForKind},
		{wager.KindWin, "10.00", "", nil},
		{wager.KindWin, "10.00", "bet-1", nil},
		{wager.KindLoss, "0.00", "", nil},
		{wager.KindLoss, "0.00", "bet-1", nil},
		{wager.KindLoss, "1.00", "", wager.ErrInvalidAmountForKind},
		{wager.KindRefund, "0.00", "bet-1", wager.ErrInvalidAmountForKind},
		{wager.KindRefund, "10.00", "bet-1", nil},
		{wager.KindRefund, "10.00", "", wager.ErrReferenceRequired},
		{wager.KindRollback, "0.00", "bet-1", wager.ErrInvalidAmountForKind},
		{wager.KindRollback, "10.00", "", wager.ErrReferenceRequired},
		{wager.KindRollback, "10.00", "bet-1", nil},
		{wager.KindBet, "10.00", "bet-1", wager.ErrReferenceNotAllowed},
		{wager.KindOpening, "10.00", "", wager.ErrOpeningNotAllowed},
		{wager.KindBet, "-1.00", "", wager.ErrInvalidMoney},
	}
	for _, tc := range cases {
		_, err := wager.NewExternal(params(t, tc.kind, tc.amount, tc.ref))
		if tc.err == nil && err != nil {
			t.Errorf("%s %s ref=%q: erro inesperado %v", tc.kind, tc.amount, tc.ref, err)
		}
		if tc.err != nil && !errors.Is(err, tc.err) {
			t.Errorf("%s %s ref=%q: esperado %v, obtido %v", tc.kind, tc.amount, tc.ref, tc.err, err)
		}
	}
}

func TestInputErrorsAreClassifiable(t *testing.T) {
	_, err := wager.NewExternal(params(t, wager.KindOpening, "1.00", ""))
	var ie *wager.InputError
	if !errors.As(err, &ie) || ie.Code() != "OPENING_NOT_ALLOWED" {
		t.Fatalf("esperado InputError OPENING_NOT_ALLOWED, obtido %v", err)
	}
	if _, err := wager.ParseExternalKind("OPENING"); !errors.Is(err, wager.ErrOpeningNotAllowed) {
		t.Fatalf("ParseExternalKind(OPENING): %v", err)
	}
	if _, err := wager.ParseExternalKind("JACKPOT"); !errors.Is(err, wager.ErrInvalidKind) {
		t.Fatalf("ParseExternalKind(JACKPOT): %v", err)
	}
	p := params(t, wager.KindRefund, "1.00", "same")
	p.ExternalTransactionID = "same"
	if _, err := wager.NewExternal(p); !errors.Is(err, wager.ErrReferenceNotAllowed) {
		t.Fatalf("autorreferência: %v", err)
	}
	p = params(t, wager.KindBet, "1.00", "")
	p.RoundID = ""
	if _, err := wager.NewExternal(p); !errors.Is(err, wager.ErrInvalidRequest) {
		t.Fatalf("roundId vazio: %v", err)
	}
	p = params(t, wager.KindBet, "1.00", "")
	p.Money = money.Money{}
	if _, err := wager.NewExternal(p); !errors.Is(err, wager.ErrInvalidMoney) {
		t.Fatalf("money não inicializado: %v", err)
	}
}

func TestStateMachine(t *testing.T) {
	tx := newTx(t, wager.KindBet, "10.00", "")
	if tx.Status() != wager.StatusPending {
		t.Fatalf("estado inicial = %s", tx.Status())
	}
	if err := tx.MarkProcessed(brl(t, "90.00"), nil, now); err != nil {
		t.Fatal(err)
	}
	// Terminal: nenhuma transição.
	if err := tx.MarkRejected(wager.FailureInsufficientFunds, nil, nil, now); !errors.Is(err, wager.ErrInvalidTransition) {
		t.Fatalf("PROCESSED -> REJECTED: %v", err)
	}
	if err := tx.MarkProcessed(brl(t, "90.00"), nil, now); !errors.Is(err, wager.ErrInvalidTransition) {
		t.Fatalf("PROCESSED -> PROCESSED: %v", err)
	}
	if err := tx.MarkFailed(wager.FailureInternalPermanent, now); !errors.Is(err, wager.ErrInvalidTransition) {
		t.Fatalf("PROCESSED -> FAILED: %v", err)
	}
	evs := tx.PullEvents()
	if len(evs) != 1 || evs[0].EventType != event.TypeWagerTransactionProcessed {
		t.Fatalf("eventos = %+v", evs)
	}

	rej := newTx(t, wager.KindBet, "10.00", "")
	if err := rej.MarkRejected(wager.FailureInsufficientFunds, nil, nil, now); err != nil {
		t.Fatal(err)
	}
	if err := rej.MarkPendingReference(now, now, now); !errors.Is(err, wager.ErrInvalidTransition) {
		t.Fatalf("REJECTED -> PENDING_REFERENCE: %v", err)
	}
	if evs := rej.PullEvents(); len(evs) != 1 || evs[0].EventType != event.TypeWagerTransactionRejected {
		t.Fatalf("eventos = %+v", evs)
	}

	failed := newTx(t, wager.KindBet, "10.00", "")
	if err := failed.MarkFailed(wager.FailureInternalPermanent, now); err != nil {
		t.Fatal(err)
	}
	if err := failed.MarkProcessed(brl(t, "1.00"), nil, now); !errors.Is(err, wager.ErrInvalidTransition) {
		t.Fatalf("FAILED -> PROCESSED: %v", err)
	}
	if len(failed.PullEvents()) != 0 {
		t.Fatal("FAILED não emite evento de integração")
	}
}

func TestPendingReferenceTransitions(t *testing.T) {
	tx := newTx(t, wager.KindRollback, "10.00", "bet-1")
	next := now.Add(time.Second)
	exp := now.Add(time.Minute)
	if err := tx.MarkPendingReference(next, exp, now); err != nil {
		t.Fatal(err)
	}
	if tx.Status() != wager.StatusPendingReference || tx.Attempts() != 0 {
		t.Fatalf("estado = %s tentativas = %d", tx.Status(), tx.Attempts())
	}
	// Reagendamento: incrementa tentativas sem novo evento e sem alterar a expiração.
	if err := tx.MarkPendingReference(next.Add(2*time.Second), exp.Add(time.Hour), now); err != nil {
		t.Fatal(err)
	}
	if tx.Attempts() != 1 || !tx.ExpiresAt().Equal(exp) {
		t.Fatalf("tentativas = %d expira = %v", tx.Attempts(), tx.ExpiresAt())
	}
	evs := tx.PullEvents()
	if len(evs) != 1 || evs[0].EventType != event.TypeWagerTransactionPendingReference {
		t.Fatalf("eventos = %+v", evs)
	}
	if err := tx.MarkRejected(wager.FailureReferenceNotFound, nil, nil, now); err != nil {
		t.Fatal(err)
	}
	if tx.Snapshot().FailureCode != wager.FailureReferenceNotFound {
		t.Fatalf("failureCode = %s", tx.Snapshot().FailureCode)
	}

	bet := newTx(t, wager.KindBet, "10.00", "")
	if err := bet.MarkPendingReference(next, exp, now); !errors.Is(err, wager.ErrInvalidTransition) {
		t.Fatalf("BET não depende de referência: %v", err)
	}
}

func TestOpeningTransaction(t *testing.T) {
	tx, err := wager.NewOpening(uuid.New(), walletID, playerID, brl(t, "1000.00"), "corr", now)
	if err != nil {
		t.Fatal(err)
	}
	s := tx.Snapshot()
	if s.Origin != wager.OriginInternal || s.Kind != wager.KindOpening || s.ProviderID != "" ||
		s.ExternalTransactionID != "" || s.IdempotencyKey != "" || s.RoundID != "" || s.GameID != "" ||
		s.ReferenceExternalTransactionID != "" {
		t.Fatalf("metadados externos não se aplicam à abertura: %+v", s)
	}
	if err := tx.MarkRejected(wager.FailureInsufficientFunds, nil, nil, now); !errors.Is(err, wager.ErrInvalidTransition) {
		t.Fatalf("abertura não pode ser rejeitada: %v", err)
	}
	if err := tx.MarkProcessed(brl(t, "1000.00"), nil, now); err != nil {
		t.Fatal(err)
	}
	evs := tx.PullEvents()
	if len(evs) != 1 {
		t.Fatalf("eventos = %d", len(evs))
	}
	data := evs[0].Data.(event.WagerTransactionProcessedData)
	if data.Origin != "INTERNAL" || data.Kind != "OPENING" || data.ProviderID != "" || data.BalanceAfter.Amount() != "1000.00" {
		t.Fatalf("payload = %+v", data)
	}
	if _, err := wager.NewOpening(uuid.New(), walletID, playerID, brl(t, "0.00"), "corr", now); !errors.Is(err, wager.ErrInvalidAmountForKind) {
		t.Fatalf("abertura com zero: %v", err)
	}
}

func TestRehydrateValidatesAndDoesNotEmit(t *testing.T) {
	tx := newTx(t, wager.KindBet, "10.00", "")
	_ = tx.MarkProcessed(brl(t, "90.00"), nil, now)
	back, err := wager.Rehydrate(tx.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if len(back.PullEvents()) != 0 || back.Status() != wager.StatusProcessed {
		t.Fatal("reidratação não deve emitir eventos nem alterar estado")
	}
	s := tx.Snapshot()
	s.FailureCode = wager.FailureInsufficientFunds
	if _, err := wager.Rehydrate(s); !errors.Is(err, wager.ErrInvalidTransaction) {
		t.Fatalf("estado incoerente: %v", err)
	}
	s = tx.Snapshot()
	s.Origin = wager.OriginInternal
	if _, err := wager.Rehydrate(s); !errors.Is(err, wager.ErrInvalidTransaction) {
		t.Fatalf("origem incoerente: %v", err)
	}
}

func processed(t *testing.T, kind wager.Kind, amount, ref string) wager.Snapshot {
	tx := newTx(t, kind, amount, ref)
	if err := tx.MarkProcessed(brl(t, "100.00"), nil, now); err != nil {
		t.Fatal(err)
	}
	return tx.Snapshot()
}

func TestDecideRulesForFiveKinds(t *testing.T) {
	bet := processed(t, wager.KindBet, "10.00", "")
	win := processed(t, wager.KindWin, "15.00", "")
	refund := processed(t, wager.KindRefund, "10.00", "bet-x")
	loss := processed(t, wager.KindLoss, "0.00", "")

	check := func(name string, d wager.Decision, outcome wager.Outcome, mv wager.Movement, code wager.FailureCode) {
		t.Helper()
		if d.Outcome != outcome || (outcome == wager.OutcomeApply && d.Movement != mv) || d.FailureCode != code {
			t.Errorf("%s: decisão = %+v", name, d)
		}
	}

	check("BET", newTx(t, wager.KindBet, "10.00", "").Decide(nil), wager.OutcomeApply, wager.MovementDebit, "")
	check("WIN sem ref", newTx(t, wager.KindWin, "10.00", "").Decide(nil), wager.OutcomeApply, wager.MovementCredit, "")
	check("LOSS", newTx(t, wager.KindLoss, "0.00", "").Decide(nil), wager.OutcomeApply, wager.MovementNone, "")
	check("WIN ref BET", newTx(t, wager.KindWin, "99.00", "b").Decide(&wager.ReferenceState{Snapshot: bet}), wager.OutcomeApply, wager.MovementCredit, "")
	check("WIN ref WIN", newTx(t, wager.KindWin, "1.00", "w").Decide(&wager.ReferenceState{Snapshot: win}), wager.OutcomeReject, 0, wager.FailureReferenceKindNotAllowed)

	check("REFUND ref ausente", newTx(t, wager.KindRefund, "10.00", "b").Decide(nil), wager.OutcomeWait, 0, "")
	check("REFUND BET", newTx(t, wager.KindRefund, "10.00", "b").Decide(&wager.ReferenceState{Snapshot: bet}), wager.OutcomeApply, wager.MovementCredit, "")
	check("REFUND valor parcial", newTx(t, wager.KindRefund, "5.00", "b").Decide(&wager.ReferenceState{Snapshot: bet}), wager.OutcomeReject, 0, wager.FailureReferenceAmountMismatch)
	check("REFUND de WIN", newTx(t, wager.KindRefund, "15.00", "w").Decide(&wager.ReferenceState{Snapshot: win}), wager.OutcomeReject, 0, wager.FailureReferenceKindNotAllowed)
	check("REFUND duplicado", newTx(t, wager.KindRefund, "10.00", "b").Decide(&wager.ReferenceState{Snapshot: bet, HasProcessedReversal: true}), wager.OutcomeReject, 0, wager.FailureReferenceAlreadyReversed)

	check("ROLLBACK BET", newTx(t, wager.KindRollback, "10.00", "b").Decide(&wager.ReferenceState{Snapshot: bet}), wager.OutcomeApply, wager.MovementCredit, "")
	check("ROLLBACK WIN", newTx(t, wager.KindRollback, "15.00", "w").Decide(&wager.ReferenceState{Snapshot: win}), wager.OutcomeApply, wager.MovementDebit, "")
	check("ROLLBACK REFUND", newTx(t, wager.KindRollback, "10.00", "r").Decide(&wager.ReferenceState{Snapshot: refund}), wager.OutcomeApply, wager.MovementDebit, "")
	check("ROLLBACK LOSS", newTx(t, wager.KindRollback, "1.00", "l").Decide(&wager.ReferenceState{Snapshot: loss}), wager.OutcomeReject, 0, wager.FailureReferenceKindNotAllowed)
	check("ROLLBACK após REFUND", newTx(t, wager.KindRollback, "10.00", "b").Decide(&wager.ReferenceState{Snapshot: bet, HasProcessedReversal: true}), wager.OutcomeReject, 0, wager.FailureReferenceAlreadyReversed)

	d := newTx(t, wager.KindRollback, "15.00", "w").Decide(&wager.ReferenceState{Snapshot: win})
	if d.InsufficientFundsCode != wager.FailureReversalInsufficientFunds {
		t.Errorf("reversão deve usar código próprio de saldo insuficiente: %s", d.InsufficientFundsCode)
	}
	if d := newTx(t, wager.KindBet, "1.00", "").Decide(nil); d.InsufficientFundsCode != wager.FailureInsufficientFunds {
		t.Errorf("BET: %s", d.InsufficientFundsCode)
	}

	// Referência pendente ou sem sucesso.
	pending := newTx(t, wager.KindRollback, "10.00", "zz")
	_ = pending.MarkPendingReference(now, now, now)
	check("ref pendente", newTx(t, wager.KindRollback, "10.00", "b").Decide(&wager.ReferenceState{Snapshot: pending.Snapshot()}), wager.OutcomeWait, 0, "")
	rejected := newTx(t, wager.KindBet, "10.00", "")
	_ = rejected.MarkRejected(wager.FailureInsufficientFunds, nil, nil, now)
	check("ref rejeitada", newTx(t, wager.KindRefund, "10.00", "b").Decide(&wager.ReferenceState{Snapshot: rejected.Snapshot()}), wager.OutcomeReject, 0, wager.FailureReferenceNotProcessed)

	// Divergências de rodada, jogador, carteira e moeda.
	for name, mutate := range map[string]func(*wager.Snapshot){
		"rodada":   func(s *wager.Snapshot) { s.RoundID = "outra" },
		"jogador":  func(s *wager.Snapshot) { s.PlayerID = uuid.New() },
		"carteira": func(s *wager.Snapshot) { s.WalletID = uuid.New() },
		"provedor": func(s *wager.Snapshot) { s.ProviderID = "provider-b" },
		"moeda":    func(s *wager.Snapshot) { s.Money, _ = money.Parse("10.00", "USD") },
	} {
		s := bet
		mutate(&s)
		check("divergência "+name, newTx(t, wager.KindRefund, "10.00", "b").Decide(&wager.ReferenceState{Snapshot: s}), wager.OutcomeReject, 0, wager.FailureReferenceMismatch)
	}
}
