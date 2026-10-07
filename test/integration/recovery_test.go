//go:build integration

package integration

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// REFUND/ROLLBACK antes da referência: fica PENDING_REFERENCE, sobrevive à
// queda da instância e é resolvido por outra instância quando a aposta chega por SQS.
func TestPendingReferenceSurvivesRestartAndResolves(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	first := startInstance(t, "pend-1", baseEnv(q))
	w := openWallet(t, first, "100.00")
	bet := "late-bet-" + uuid.NewString()
	refund := "early-refund-" + uuid.NewString()

	r := submit(t, first, "provider-a", wagerBody(w, "provider-a", refund, "REFUND", "30.00", bet), "provider-a:"+refund)
	if r.Status != http.StatusAccepted || r.str("status") != "PENDING_REFERENCE" {
		t.Fatalf("REFUND antecipado: %d %s", r.Status, r.Raw)
	}
	if queryInt(t, `SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'WagerTransactionPendingReference'`, r.str("transactionId")) != 1 {
		t.Fatal("evento WagerTransactionPendingReference ausente")
	}
	first.Kill() // queda abrupta com a pendência confirmada

	second := startInstance(t, "pend-2", baseEnv(q))
	// Replay após reinício: mesma transação, ainda pendente.
	again := submit(t, second, "provider-a", wagerBody(w, "provider-a", refund, "REFUND", "30.00", bet), "provider-a:"+refund)
	if again.str("transactionId") != r.str("transactionId") || again.Body["idempotentReplay"] != true {
		t.Fatalf("replay após reinício: %s", again.Raw)
	}
	sendSQS(t, q.WagerURL, sqsMessage("msg-"+uuid.NewString(), w, "provider-a", bet, "BET", "30.00", "", "provider-a:"+bet), w.ID)

	eventually(t, 20*time.Second, "REFUND resolvido", func() bool {
		g := call(t, "GET", second.Base+"/providers/provider-a/wagering/transactions/"+refund, token(t, "provider-a"), nil, nil)
		return g.str("status") == "PROCESSED"
	})
	if bal := walletBalance(t, w.ID); bal != 10000 {
		t.Fatalf("saldo = %d (aposta debitada e devolvida)", bal)
	}
	assertLedgerConsistent(t, w.ID)
}

// Sem a referência até o TTL: REJECTED com REFERENCE_NOT_FOUND e evento de rejeição.
func TestPendingReferenceExpires(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	e := baseEnv(q)
	e["PENDING_REFERENCE_TTL"] = "2s"
	inst := startInstance(t, "pend-exp", e)
	w := openWallet(t, inst, "100.00")
	ext := "orphan-" + uuid.NewString()
	r := submit(t, inst, "provider-a", wagerBody(w, "provider-a", ext, "ROLLBACK", "10.00", "never-"+ext), "provider-a:"+ext)
	if r.Status != http.StatusAccepted {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
	eventually(t, 15*time.Second, "expiração", func() bool {
		g := call(t, "GET", inst.Base+"/wagering/transactions/"+r.str("transactionId"), token(t, "provider-a"), nil, nil)
		return g.str("status") == "REJECTED" && g.str("failureCode") == "REFERENCE_NOT_FOUND"
	})
	if queryInt(t, `SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'WagerTransactionRejected'`, r.str("transactionId")) != 1 {
		t.Fatal("evento de rejeição ausente")
	}
	if walletBalance(t, w.ID) != 10000 {
		t.Fatal("saldo não pode mudar")
	}
}

// Reinício completo: idempotência, saldo original no replay e consistência preservados.
func TestFullRestartPreservesIdempotencyAndConsistency(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	nodes := startCluster(t, 3, q, nil)
	w := openWallet(t, nodes[0], "100.00")
	ext := "rst-" + uuid.NewString()
	first := submit(t, nodes[1], "provider-a", wagerBody(w, "provider-a", ext, "BET", "40.00", ""), "provider-a:"+ext)
	other := "rst2-" + uuid.NewString()
	submit(t, nodes[2], "provider-a", wagerBody(w, "provider-a", other, "BET", "10.00", ""), "provider-a:"+other)
	for _, n := range nodes {
		n.Kill()
	}

	fresh := startInstance(t, "rst-new", baseEnv(q))
	r := submit(t, fresh, "provider-a", wagerBody(w, "provider-a", ext, "BET", "40.00", ""), "provider-a:"+ext)
	if r.Status != http.StatusOK || r.Body["idempotentReplay"] != true || r.str("transactionId") != first.str("transactionId") || r.str("balance", "amount") != "60.00" {
		t.Fatalf("replay após reinício: %d %s", r.Status, r.Raw)
	}
	conflict := submit(t, fresh, "provider-a", wagerBody(w, "provider-a", ext, "BET", "41.00", ""), "provider-a:"+ext)
	if conflict.Status != http.StatusConflict {
		t.Fatalf("conflito após reinício: %d", conflict.Status)
	}
	rec := call(t, "POST", fresh.Base+"/wallets/"+w.ID+"/reconciliation", token(t, "wallet-service"), nil, nil)
	if rec.Body["consistent"] != true || rec.str("storedBalance", "amount") != "50.00" {
		t.Fatalf("reconciliação: %s", rec.Raw)
	}
}

// PostgreSQL indisponível: respostas 503 distinguíveis, readiness DOWN e,
// após a volta, a mesma requisição é aplicada uma única vez.
func TestPostgresUnavailable(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	proxy := newProxy(t, pgHost)
	e := baseEnv(q)
	e["DATABASE_URL"] = appURL(proxy.Addr())
	e["SQS_CONSUMER_ENABLED"] = "false"
	inst := startInstance(t, "pg-down", e)
	w := openWallet(t, inst, "100.00")
	ext := "pgd-" + uuid.NewString()
	body := wagerBody(w, "provider-a", ext, "BET", "10.00", "")

	proxy.SetUp(false)
	r := submit(t, inst, "provider-a", body, "provider-a:"+ext)
	if r.Status != http.StatusServiceUnavailable || r.str("error", "code") != "SERVICE_UNAVAILABLE" || r.Body["error"].(map[string]any)["retryable"] != true {
		t.Fatalf("com banco indisponível: %d %s", r.Status, r.Raw)
	}
	ready := call(t, "GET", inst.Base+"/health/ready", "", nil, nil)
	if ready.Status != http.StatusServiceUnavailable || !strings.Contains(ready.Raw, `"postgres":"DOWN"`) {
		t.Fatalf("readiness: %d %s", ready.Status, ready.Raw)
	}
	if live := call(t, "GET", inst.Base+"/health/live", "", nil, nil); live.Status != http.StatusOK {
		t.Fatal("liveness deve permanecer UP")
	}

	proxy.SetUp(true)
	eventually(t, 15*time.Second, "banco de volta", func() bool {
		return call(t, "GET", inst.Base+"/health/ready", "", nil, nil).Status == http.StatusOK
	})
	ok := submit(t, inst, "provider-a", body, "provider-a:"+ext)
	if ok.Status != http.StatusCreated && ok.Status != http.StatusOK {
		t.Fatalf("após recuperação: %d %s", ok.Status, ok.Raw)
	}
	if n := queryInt(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.ID); n != 1 {
		t.Fatalf("débitos = %d", n)
	}
}

// SQS indisponível: readiness DOWN, eventos acumulam na outbox sem perda e são
// publicados quando o broker volta; o consumidor retoma o consumo.
func TestSQSUnavailable(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	proxy := newProxy(t, strings.TrimPrefix(sqsEndpoint, "http://"))
	e := baseEnv(q)
	e["AWS_ENDPOINT_URL"] = "http://" + proxy.Addr()
	inst := startInstance(t, "sqs-down", e)

	proxy.SetUp(false)
	eventually(t, 10*time.Second, "readiness DOWN", func() bool {
		r := call(t, "GET", inst.Base+"/health/ready", "", nil, nil)
		return r.Status == http.StatusServiceUnavailable && strings.Contains(r.Raw, `"sqs":"DOWN"`)
	})
	// A API continua aceitando operações: os eventos ficam na outbox.
	w := openWallet(t, inst, "100.00")
	ext := "sqsd-" + uuid.NewString()
	if r := submit(t, inst, "provider-a", wagerBody(w, "provider-a", ext, "BET", "10.00", ""), "provider-a:"+ext); r.Status != http.StatusCreated {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
	ids := walletEventIDs(t, []string{w.ID})
	time.Sleep(time.Second)
	if unpublished(t, ids) != len(ids) {
		t.Fatal("eventos não podem ser marcados como publicados sem o broker")
	}

	proxy.SetUp(true)
	eventually(t, 30*time.Second, "eventos publicados após a volta do SQS", func() bool { return unpublished(t, ids) == 0 })
	got := receivedEvents(t, q)
	for id := range ids {
		if len(got[id]) != 1 {
			t.Fatalf("evento %s recebido %d vezes", id, len(got[id]))
		}
	}
	// Consumidor retomado.
	ext2 := "sqsd2-" + uuid.NewString()
	sendSQS(t, q.WagerURL, sqsMessage("msg-"+uuid.NewString(), w, "provider-a", ext2, "BET", "5.00", "", "k-"+ext2), w.ID)
	eventually(t, 20*time.Second, "mensagem consumida após recuperação", func() bool {
		return queryInt(t, `SELECT COUNT(*) FROM wager_transactions WHERE external_transaction_id = $1 AND status = 'PROCESSED'`, ext2) == 1
	})
}
