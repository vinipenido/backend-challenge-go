package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/vinipenido/backend-challenge-go/internal/app"
	"github.com/vinipenido/backend-challenge-go/internal/domain/event"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wager"
)

type harness struct {
	uow     *memUOW
	clock   *fakeClock
	wallets *app.WalletService
	wagers  *app.WageringService
}

func newHarness() *harness {
	uow := newMemUOW()
	clock := &fakeClock{now: time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return &harness{
		uow:     uow,
		clock:   clock,
		wallets: app.NewWalletService(uow, clock, app.UUIDv7{}, app.NopMetrics{}, log),
		wagers: app.NewWageringService(uow, clock, app.UUIDv7{}, app.NopMetrics{}, log, app.WageringConfig{
			PendingReferenceTTL: time.Minute, PendingReferenceMaxAttempts: 50,
			PendingBackoffBase: time.Second, PendingBackoffMax: 4 * time.Second,
		}),
	}
}

func str(s string) *string { return &s }

func (h *harness) open(t *testing.T, amount string) app.WalletView {
	t.Helper()
	w, err := h.wallets.Open(context.Background(), app.OpenWalletCommand{
		PlayerID:       uuid.NewString(),
		InitialBalance: &app.MoneyInput{Amount: str(amount), Currency: str("BRL")},
		CorrelationID:  "corr-open",
	})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func input(w app.WalletView, kind, ext, amount, ref string) app.WagerRequestInput {
	return app.WagerRequestInput{
		ProviderID: "provider-a", ExternalTransactionID: ext, PlayerID: w.PlayerID.String(),
		WalletID: w.ID.String(), RoundID: "round-1", GameID: "game-1", Kind: kind,
		Money: &app.MoneyInput{Amount: str(amount), Currency: str("BRL")}, ReferenceExternalTransactionID: ref,
	}
}

func (h *harness) send(t *testing.T, in app.WagerRequestInput, key string) (app.ProcessResult, error) {
	t.Helper()
	req, err := app.ParseWagerRequest(in, key)
	if err != nil {
		return app.ProcessResult{}, err
	}
	return h.wagers.Process(context.Background(), app.ProcessCommand{Request: req, Channel: app.ChannelHTTP, CorrelationID: "corr"})
}

func (h *harness) mustSend(t *testing.T, in app.WagerRequestInput) app.ProcessResult {
	t.Helper()
	res, err := h.send(t, in, in.ProviderID+":"+in.ExternalTransactionID)
	if err != nil {
		t.Fatalf("send %s %s: %v", in.Kind, in.ExternalTransactionID, err)
	}
	return res
}

func (h *harness) balance(t *testing.T, w app.WalletView) string {
	t.Helper()
	v, err := h.wallets.Get(context.Background(), w.ID)
	if err != nil {
		t.Fatal(err)
	}
	return v.Balance.Amount()
}

func countEvents(s *memState, typ event.Type) int {
	n := 0
	for _, e := range s.outbox {
		if e.EventType == typ {
			n++
		}
	}
	return n
}

func TestOpenWalletPositiveAndZero(t *testing.T) {
	h := newHarness()
	w := h.open(t, "1000.00")
	if w.Version != 1 || w.Balance.Amount() != "1000.00" {
		t.Fatalf("carteira = %+v", w)
	}
	st := h.uow.snapshot()
	if len(st.ledger[w.ID]) != 1 || countEvents(st, event.TypeWalletBalanceChanged) != 1 || countEvents(st, event.TypeWagerTransactionProcessed) != 1 {
		t.Fatalf("abertura positiva deve gerar 1 lançamento e 2 eventos: ledger=%d outbox=%d", len(st.ledger[w.ID]), len(st.outbox))
	}
	var opening wager.Snapshot
	for _, s := range st.txs {
		opening = s
	}
	if opening.Kind != wager.KindOpening || opening.Status != wager.StatusProcessed || opening.Origin != wager.OriginInternal {
		t.Fatalf("OPENING = %+v", opening)
	}

	z := h.open(t, "0.00")
	st = h.uow.snapshot()
	if len(st.ledger[z.ID]) != 0 || len(st.txs) != 1 || len(st.outbox) != 2 {
		t.Fatal("saldo zero não cria OPENING, ledger nem eventos")
	}

	_, err := h.wallets.Open(context.Background(), app.OpenWalletCommand{
		PlayerID: w.PlayerID.String(), InitialBalance: &app.MoneyInput{Amount: str("1.00"), Currency: str("BRL")},
	})
	if !errors.Is(err, app.ErrWalletAlreadyExists) {
		t.Fatalf("segunda carteira para o mesmo jogador/moeda: %v", err)
	}
}

func TestBetReplayReturnsOriginalBalance(t *testing.T) {
	h := newHarness()
	w := h.open(t, "1000.00")
	bet := input(w, "BET", "transaction-123", "25.00", "")

	first := h.mustSend(t, bet)
	if first.Replay || first.Transaction.Status != wager.StatusProcessed || first.Transaction.ResultBalance.Amount() != "975.00" {
		t.Fatalf("primeiro envio = %+v", first)
	}
	h.mustSend(t, input(w, "BET", "transaction-124", "100.00", ""))

	again := h.mustSend(t, bet)
	if !again.Replay || again.Transaction.ID != first.Transaction.ID {
		t.Fatalf("replay = %+v", again)
	}
	if again.Transaction.ResultBalance.Amount() != "975.00" {
		t.Fatalf("replay deve devolver o saldo original, obtido %s", again.Transaction.ResultBalance.Amount())
	}
	if got := h.balance(t, w); got != "875.00" {
		t.Fatalf("saldo = %s", got)
	}
}

func TestIdempotencyConflicts(t *testing.T) {
	h := newHarness()
	w := h.open(t, "100.00")
	bet := input(w, "BET", "tx-1", "10.00", "")
	h.mustSend(t, bet)

	changed := input(w, "BET", "tx-1", "11.00", "")
	if _, err := h.send(t, changed, "provider-a:tx-1"); !errors.Is(err, app.ErrIdempotencyConflict) {
		t.Fatalf("mesma chave com conteúdo diferente: %v", err)
	}
	if _, err := h.send(t, bet, "outra-chave"); !errors.Is(err, app.ErrExternalTransactionExists) {
		t.Fatalf("mesma operação com outra chave: %v", err)
	}
	if got := h.balance(t, w); got != "90.00" {
		t.Fatalf("conflitos não podem movimentar saldo: %s", got)
	}
}

func TestCanonicalHashEquivalence(t *testing.T) {
	httpBody := `{"money":{"currency":"BRL","amount":"25.00"},"kind":"BET","walletId":"0192F291-27DD-7D3F-8071-5F8685DEEF37","roundId":"round-987","providerId":"provider-a","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","gameId":"fortune-chimp","externalTransactionId":"transaction-123"}`
	sqsData := `{"providerId":"provider-a","externalTransactionId":"transaction-123","idempotencyKey":"provider-a:transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}`
	var a, b app.WagerRequestInput
	if err := json.Unmarshal([]byte(httpBody), &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(sqsData), &b); err != nil {
		t.Fatal(err)
	}
	ra, err := app.ParseWagerRequest(a, "provider-a:transaction-123")
	if err != nil {
		t.Fatal(err)
	}
	rb, err := app.ParseWagerRequest(b, "outra-chave")
	if err != nil {
		t.Fatal(err)
	}
	if ra.PayloadHash != rb.PayloadHash || len(ra.PayloadHash) != 64 {
		t.Fatalf("hashes diferentes: %s %s", ra.PayloadHash, rb.PayloadHash)
	}
	b.Money.Amount = str("25.01")
	rc, _ := app.ParseWagerRequest(b, "k")
	if rc.PayloadHash == ra.PayloadHash {
		t.Fatal("conteúdo diferente deve gerar hash diferente")
	}
}

func TestRequestValidation(t *testing.T) {
	w := app.WalletView{ID: uuid.New(), PlayerID: uuid.New()}
	cases := map[string]struct {
		in   app.WagerRequestInput
		key  string
		code string
	}{
		"sem chave":          {input(w, "BET", "x", "1.00", ""), "", "INVALID_REQUEST"},
		"OPENING":            {input(w, "OPENING", "x", "1.00", ""), "k", "OPENING_NOT_ALLOWED"},
		"tipo desconhecido":  {input(w, "JACKPOT", "x", "1.00", ""), "k", "INVALID_KIND"},
		"valor negativo":     {input(w, "BET", "x", "-1.00", ""), "k", "INVALID_MONEY"},
		"notação científica": {input(w, "BET", "x", "1e2", ""), "k", "INVALID_MONEY"},
		"escala excedente":   {input(w, "BET", "x", "1.001", ""), "k", "INVALID_MONEY"},
		"LOSS não zero":      {input(w, "LOSS", "x", "1.00", ""), "k", "INVALID_AMOUNT_FOR_KIND"},
		"BET zero":           {input(w, "BET", "x", "0.00", ""), "k", "INVALID_AMOUNT_FOR_KIND"},
		"REFUND sem ref":     {input(w, "REFUND", "x", "1.00", ""), "k", "REFERENCE_REQUIRED"},
	}
	bad := input(w, "BET", "x", "1.00", "")
	bad.WalletID = "not-a-uuid"
	cases["walletId inválido"] = struct {
		in   app.WagerRequestInput
		key  string
		code string
	}{bad, "k", "INVALID_REQUEST"}

	for name, tc := range cases {
		_, err := app.ParseWagerRequest(tc.in, tc.key)
		var ie *wager.InputError
		if !errors.As(err, &ie) || ie.Code() != tc.code {
			t.Errorf("%s: esperado %s, obtido %v", name, tc.code, err)
		}
	}
}

func TestTwoConcurrentBetsOf80(t *testing.T) {
	h := newHarness()
	w := h.open(t, "100.00")
	var wg sync.WaitGroup
	results := make([]app.ProcessResult, 2)
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = h.mustSend(t, input(w, "BET", "bet-80-"+string(rune('a'+i)), "80.00", ""))
		}()
	}
	wg.Wait()
	statuses := map[wager.Status]int{}
	for _, r := range results {
		statuses[r.Transaction.Status]++
		if r.Transaction.Status == wager.StatusRejected && r.Transaction.FailureCode != wager.FailureInsufficientFunds {
			t.Fatalf("código = %s", r.Transaction.FailureCode)
		}
	}
	if statuses[wager.StatusProcessed] != 1 || statuses[wager.StatusRejected] != 1 {
		t.Fatalf("resultados = %v", statuses)
	}
	if got := h.balance(t, w); got != "20.00" {
		t.Fatalf("saldo = %s", got)
	}
	debits := 0
	for _, e := range h.uow.snapshot().ledger[w.ID] {
		if e.Direction() == "DEBIT" {
			debits++
		}
	}
	if debits != 1 {
		t.Fatalf("débitos = %d", debits)
	}
}

func TestSameBet50TimesInParallel(t *testing.T) {
	h := newHarness()
	w := h.open(t, "100.00")
	bet := input(w, "BET", "dup-1", "10.00", "")
	var wg sync.WaitGroup
	var mu sync.Mutex
	fresh := 0
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := h.mustSend(t, bet)
			if !r.Replay {
				mu.Lock()
				fresh++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if fresh != 1 {
		t.Fatalf("aplicações efetivas = %d, esperado 1", fresh)
	}
	if got := h.balance(t, w); got != "90.00" {
		t.Fatalf("saldo = %s", got)
	}
}

func TestLossDoesNotMoveMoney(t *testing.T) {
	h := newHarness()
	w := h.open(t, "100.00")
	before := h.uow.snapshot()
	res := h.mustSend(t, input(w, "LOSS", "loss-1", "0.00", ""))
	if res.Transaction.Status != wager.StatusProcessed || res.Transaction.ResultBalance.Amount() != "100.00" {
		t.Fatalf("LOSS = %+v", res.Transaction)
	}
	after := h.uow.snapshot()
	if len(after.ledger[w.ID]) != len(before.ledger[w.ID]) {
		t.Fatal("LOSS não cria ledger")
	}
	if after.wallets[w.ID].Version != 1 {
		t.Fatalf("LOSS não altera a versão: %d", after.wallets[w.ID].Version)
	}
	if countEvents(after, event.TypeWalletBalanceChanged) != countEvents(before, event.TypeWalletBalanceChanged) {
		t.Fatal("LOSS não emite WalletBalanceChanged")
	}
	if countEvents(after, event.TypeWagerTransactionProcessed) != countEvents(before, event.TypeWagerTransactionProcessed)+1 {
		t.Fatal("LOSS emite WagerTransactionProcessed")
	}
}

func TestReversalPolicies(t *testing.T) {
	h := newHarness()
	w := h.open(t, "100.00")
	h.mustSend(t, input(w, "BET", "bet-1", "30.00", ""))
	if r := h.mustSend(t, input(w, "REFUND", "refund-1", "30.00", "bet-1")); r.Transaction.Status != wager.StatusProcessed {
		t.Fatalf("REFUND = %+v", r.Transaction)
	}
	if got := h.balance(t, w); got != "100.00" {
		t.Fatalf("saldo após refund = %s", got)
	}
	// Segunda reversão da mesma aposta (mesmo tipo ou outro) é rejeitada.
	for _, tc := range []struct{ kind, ext string }{{"REFUND", "refund-2"}, {"ROLLBACK", "rb-bet-1"}} {
		r := h.mustSend(t, input(w, tc.kind, tc.ext, "30.00", "bet-1"))
		if r.Transaction.Status != wager.StatusRejected || r.Transaction.FailureCode != wager.FailureReferenceAlreadyReversed {
			t.Fatalf("%s duplicado = %+v", tc.kind, r.Transaction)
		}
	}
	// ROLLBACK do REFUND debita de volta.
	if r := h.mustSend(t, input(w, "ROLLBACK", "rb-refund-1", "30.00", "refund-1")); r.Transaction.Status != wager.StatusProcessed {
		t.Fatalf("ROLLBACK do REFUND = %+v", r.Transaction)
	}
	if got := h.balance(t, w); got != "70.00" {
		t.Fatalf("saldo = %s", got)
	}
	// Valor parcial é rejeitado.
	h.mustSend(t, input(w, "BET", "bet-2", "10.00", ""))
	if r := h.mustSend(t, input(w, "REFUND", "refund-3", "5.00", "bet-2")); r.Transaction.FailureCode != wager.FailureReferenceAmountMismatch {
		t.Fatalf("parcial = %+v", r.Transaction)
	}
}

func TestReversalInsufficientFundsHasOwnCode(t *testing.T) {
	h := newHarness()
	w := h.open(t, "0.00")
	h.mustSend(t, input(w, "WIN", "win-1", "50.00", ""))
	h.mustSend(t, input(w, "BET", "bet-1", "40.00", ""))
	r := h.mustSend(t, input(w, "ROLLBACK", "rb-win-1", "50.00", "win-1"))
	if r.Transaction.Status != wager.StatusRejected || r.Transaction.FailureCode != wager.FailureReversalInsufficientFunds {
		t.Fatalf("ROLLBACK sem saldo = %+v", r.Transaction)
	}
	b := h.mustSend(t, input(w, "BET", "bet-2", "40.00", ""))
	if b.Transaction.FailureCode != wager.FailureInsufficientFunds {
		t.Fatalf("BET sem saldo = %+v", b.Transaction)
	}
	if got := h.balance(t, w); got != "10.00" {
		t.Fatalf("saldo = %s", got)
	}
}

func TestPendingReferenceResolvedLater(t *testing.T) {
	h := newHarness()
	w := h.open(t, "100.00")
	rb := h.mustSend(t, input(w, "ROLLBACK", "rb-1", "20.00", "bet-late"))
	if rb.Transaction.Status != wager.StatusPendingReference {
		t.Fatalf("ROLLBACK antes da aposta = %+v", rb.Transaction)
	}
	if countEvents(h.uow.snapshot(), event.TypeWagerTransactionPendingReference) != 1 {
		t.Fatal("evento de pendência ausente")
	}
	// A aposta chega depois e antecipa a pendência.
	h.mustSend(t, input(w, "BET", "bet-late", "20.00", ""))
	n, err := h.wagers.ResolvePending(context.Background(), 10)
	if err != nil || n != 1 {
		t.Fatalf("ResolvePending = %d, %v", n, err)
	}
	replay := h.mustSend(t, input(w, "ROLLBACK", "rb-1", "20.00", "bet-late"))
	if replay.Transaction.Status != wager.StatusProcessed {
		t.Fatalf("após resolução = %+v", replay.Transaction)
	}
	if got := h.balance(t, w); got != "100.00" {
		t.Fatalf("saldo = %s", got)
	}
}

func TestPendingReferenceExpires(t *testing.T) {
	h := newHarness()
	w := h.open(t, "100.00")
	h.mustSend(t, input(w, "REFUND", "refund-orphan", "20.00", "never-arrives"))
	for range 30 {
		h.clock.Advance(5 * time.Second)
		if _, err := h.wagers.ResolvePending(context.Background(), 10); err != nil {
			t.Fatal(err)
		}
	}
	r := h.mustSend(t, input(w, "REFUND", "refund-orphan", "20.00", "never-arrives"))
	if r.Transaction.Status != wager.StatusRejected || r.Transaction.FailureCode != wager.FailureReferenceNotFound {
		t.Fatalf("expiração = %+v", r.Transaction)
	}
	if countEvents(h.uow.snapshot(), event.TypeWagerTransactionRejected) != 1 {
		t.Fatal("expiração deve produzir WagerTransactionRejected")
	}
}

func TestInboxDeduplication(t *testing.T) {
	h := newHarness()
	w := h.open(t, "100.00")
	req, err := app.ParseWagerRequest(input(w, "BET", "sqs-1", "10.00", ""), "provider-a:sqs-1")
	if err != nil {
		t.Fatal(err)
	}
	cmd := app.ProcessCommand{Request: req, Channel: app.ChannelSQS, CorrelationID: "msg-1", CausationID: "msg-1",
		Inbox: &app.InboxMessage{Consumer: "wager-consumer", MessageID: "msg-1", Hash: strings.Repeat("a", 64), ReceivedAt: h.clock.Now()}}
	first, err := h.wagers.Process(context.Background(), cmd)
	if err != nil || first.Replay {
		t.Fatalf("primeira entrega: %+v %v", first, err)
	}
	again, err := h.wagers.Process(context.Background(), cmd)
	if err != nil || !again.DuplicateMessage || again.Transaction.ID != first.Transaction.ID {
		t.Fatalf("reentrega: %+v %v", again, err)
	}
	cmd.Inbox.Hash = strings.Repeat("b", 64)
	if _, err := h.wagers.Process(context.Background(), cmd); !errors.Is(err, app.ErrInboxHashConflict) {
		t.Fatalf("mesmo messageId com outro conteúdo: %v", err)
	}
	// Mesma operação via HTTP (outra entrada) é replay financeiro.
	r := h.mustSend(t, input(w, "BET", "sqs-1", "10.00", ""))
	if !r.Replay {
		t.Fatal("HTTP depois de SQS deve ser replay")
	}
	if got := h.balance(t, w); got != "90.00" {
		t.Fatalf("saldo = %s", got)
	}
}

func TestReconcileAndLedgerPagination(t *testing.T) {
	h := newHarness()
	w := h.open(t, "1000.00")
	h.mustSend(t, input(w, "BET", "transaction-123", "25.00", ""))
	rec, err := h.wallets.Reconcile(context.Background(), w.ID, "corr")
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Consistent || rec.CheckedEntries != 2 || rec.StoredBalance.Amount() != "975.00" || rec.Difference.Amount() != "0.00" {
		t.Fatalf("reconciliação = %+v", rec)
	}

	for i := range 5 {
		h.mustSend(t, input(w, "WIN", "win-"+string(rune('a'+i)), "1.00", ""))
	}
	var all []int64
	cursor := ""
	for {
		page, err := h.wallets.ListLedger(context.Background(), w.ID, cursor, 3)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Entries {
			all = append(all, e.WalletVersion())
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(all) != 7 || all[0] != 1 || all[6] != 7 {
		t.Fatalf("paginação = %v", all)
	}
	if _, err := h.wallets.ListLedger(context.Background(), uuid.New(), cursor, 3); !errors.Is(err, app.ErrInvalidCursor) {
		t.Fatalf("cursor de outra carteira: %v", err)
	}
}

func TestWalletMismatchRejections(t *testing.T) {
	h := newHarness()
	w := h.open(t, "100.00")
	in := input(w, "BET", "pm-1", "10.00", "")
	in.PlayerID = uuid.NewString()
	if r := h.mustSend(t, in); r.Transaction.FailureCode != wager.FailurePlayerWalletMismatch {
		t.Fatalf("jogador divergente = %+v", r.Transaction)
	}
	usd := input(w, "BET", "cur-1", "10.00", "")
	usd.Money.Currency = str("USD")
	if r := h.mustSend(t, usd); r.Transaction.FailureCode != wager.FailureCurrencyMismatch {
		t.Fatalf("moeda divergente = %+v", r.Transaction)
	}
	missing := input(app.WalletView{ID: uuid.New(), PlayerID: w.PlayerID}, "BET", "nw-1", "10.00", "")
	if _, err := h.send(t, missing, "k-nw"); !errors.Is(err, app.ErrWalletNotFound) {
		t.Fatalf("carteira inexistente: %v", err)
	}
}
