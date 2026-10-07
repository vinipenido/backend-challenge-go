//go:build integration

package integration

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/uuid"
)

func dlqReasons(t *testing.T, q queues, idle time.Duration) map[string]int {
	t.Helper()
	out := map[string]int{}
	for _, m := range drainQueue(t, q.DLQURL, idle) {
		if a, ok := m.MessageAttributes["dlqReason"]; ok {
			out[aws.ToString(a.StringValue)]++
		} else {
			out["redrive"]++ // movida pelo SQS após esgotar maxReceiveCount
		}
	}
	return out
}

func TestSQSInboxDeduplicationAndDLQ(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	inst := startInstance(t, "sqs-1", baseEnv(q))
	w := openWallet(t, inst, "100.00")

	ext := "sqs-" + uuid.NewString()
	msgID := "msg-" + uuid.NewString()
	body := sqsMessage(msgID, w, "provider-a", ext, "BET", "10.00", "", "provider-a:"+ext)

	// Mesma mensagem entregue duas vezes (o SQS não deduplica: dedup ids distintos).
	sendSQS(t, q.WagerURL, body, w.ID)
	sendSQS(t, q.WagerURL, body, w.ID)
	// Mesmo messageId com conteúdo diferente: conflito de hash -> DLQ.
	conflicting := strings.Replace(body, `"10.00"`, `"11.00"`, 1)
	sendSQS(t, q.WagerURL, conflicting, w.ID)
	// Mensagens inválidas -> DLQ.
	sendSQS(t, q.WagerURL, `{not json`, "g1")
	sendSQS(t, q.WagerURL, sqsMessage("msg-"+uuid.NewString(), w, "provider-a", "op-"+uuid.NewString(), "OPENING", "10.00", "", "k-op"), w.ID)
	sendSQS(t, q.WagerURL, sqsMessage("msg-"+uuid.NewString(), w, "provider-z", "pz-"+uuid.NewString(), "BET", "1.00", "", "k-pz"), w.ID)
	// Rejeição de negócio confirmada é terminal: removida da fila, sem DLQ.
	big := "big-" + uuid.NewString()
	sendSQS(t, q.WagerURL, sqsMessage("msg-"+uuid.NewString(), w, "provider-a", big, "BET", "500.00", "", "provider-a:"+big), w.ID)

	eventually(t, 20*time.Second, "fila de entrada vazia", func() bool {
		v, f := queueDepth(t, q.WagerURL)
		return v == 0 && f == 0
	})
	reasons := dlqReasons(t, q, 2*time.Second)
	want := map[string]int{"message_hash_conflict": 1, "invalid_message": 1, "invalid_payload": 1, "untrusted_provider": 1}
	for k, v := range want {
		if reasons[k] != v {
			t.Fatalf("DLQ: %v, esperado %v", reasons, want)
		}
	}
	if n := queryInt(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.ID); n != 1 {
		t.Fatalf("débitos = %d", n)
	}
	if n := queryInt(t, `SELECT COUNT(*) FROM inbox_messages WHERE message_id = $1`, msgID); n != 1 {
		t.Fatalf("inbox = %d", n)
	}
	if inst.Metric("wager_duplicates_total", `reason="inbox_duplicate"`) != 1 {
		t.Fatal("reentrega deveria ser contabilizada como duplicata da inbox")
	}
	if inst.Metric("sqs_dlq_messages_total") != 4 {
		t.Fatalf("métrica de DLQ = %v", inst.Metric("sqs_dlq_messages_total"))
	}
	r := call(t, "GET", inst.Base+"/providers/provider-a/wagering/transactions/"+big, token(t, "provider-a"), nil, nil)
	if r.str("status") != "REJECTED" || r.str("failureCode") != "INSUFFICIENT_FUNDS" {
		t.Fatalf("rejeição via SQS: %s", r.Raw)
	}

	// Cruzamento HTTP após SQS: replay com o saldo original.
	h := submit(t, inst, "provider-a", wagerBody(w, "provider-a", ext, "BET", "10.00", ""), "provider-a:"+ext)
	if h.Status != http.StatusOK || h.Body["idempotentReplay"] != true || h.str("balance", "amount") != "90.00" {
		t.Fatalf("HTTP após SQS: %d %s", h.Status, h.Raw)
	}
	assertLedgerConsistent(t, w.ID)
}

// Falhas transitórias usam retry com backoff; esgotado maxReceiveCount, o
// redrive do SQS leva a mensagem à DLQ, sem efeito financeiro.
func TestSQSTransientFailuresRetryThenDLQ(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	e := baseEnv(q)
	e["FAULT_ERROR_POINTS"] = "sqs.before_process"
	inst := startInstance(t, "sqs-retry", e)
	w := openWallet(t, inst, "100.00")
	ext := "retry-" + uuid.NewString()
	sendSQS(t, q.WagerURL, sqsMessage("msg-"+uuid.NewString(), w, "provider-a", ext, "BET", "10.00", "", "k-"+ext), w.ID)

	eventually(t, 40*time.Second, "mensagem na DLQ após tentativas", func() bool {
		v, _ := queueDepth(t, q.DLQURL)
		return v == 1
	})
	if got := inst.Metric("sqs_messages_total", `outcome="retry"`); got < 3 {
		t.Fatalf("tentativas = %v", got)
	}
	if n := queryInt(t, `SELECT COUNT(*) FROM wager_transactions WHERE external_transaction_id = $1`, ext); n != 0 {
		t.Fatal("falha transitória não pode deixar efeito")
	}
	if walletBalance(t, w.ID) != 10000 {
		t.Fatal("saldo alterado")
	}
}

// Encerramento abrupto depois do COMMIT e antes da remoção da mensagem: outra
// instância recebe a reentrega, a inbox a reconhece e ela é removida sem novo débito.
func TestSQSCrashAfterCommitBeforeDelete(t *testing.T) {
	q := createQueues(t, 5, 5*time.Second)
	e := baseEnv(q)
	e["FAULT_CRASH_POINTS"] = "sqs.after_commit_before_delete"
	e["OUTBOX_PUBLISHER_ENABLED"] = "false"
	crashing := startInstance(t, "sqs-crash", e)
	w := openWallet(t, crashing, "100.00")
	ext := "crash-" + uuid.NewString()
	msgID := "msg-" + uuid.NewString()
	sendSQS(t, q.WagerURL, sqsMessage(msgID, w, "provider-a", ext, "BET", "10.00", "", "k-"+ext), w.ID)

	if !crashing.WaitExit(20 * time.Second) {
		t.Fatal("a instância deveria ter caído no ponto injetado")
	}
	// O commit aconteceu: transação, débito e inbox persistidos; mensagem ainda na fila.
	if n := queryInt(t, `SELECT COUNT(*) FROM inbox_messages WHERE message_id = $1`, msgID); n != 1 {
		t.Fatalf("inbox = %d", n)
	}
	if walletBalance(t, w.ID) != 9000 {
		t.Fatal("o débito confirmado deveria existir")
	}
	if v, f := queueDepth(t, q.WagerURL); v+f != 1 {
		t.Fatalf("a mensagem deveria continuar na fila (visíveis=%d, em voo=%d)", v, f)
	}

	survivor := startInstance(t, "sqs-survivor", baseEnv(q))
	eventually(t, 30*time.Second, "reentrega tratada e removida", func() bool {
		v, f := queueDepth(t, q.WagerURL)
		return v == 0 && f == 0 && survivor.Metric("wager_duplicates_total", `reason="inbox_duplicate"`) == 1
	})
	if n := queryInt(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.ID); n != 1 {
		t.Fatalf("débitos = %d", n)
	}
	assertLedgerConsistent(t, w.ID)
}

// SIGTERM: a instância para de buscar mensagens, conclui ou devolve o trabalho
// em andamento e termina com sucesso; nada é perdido nem duplicado.
func TestSQSGracefulShutdown(t *testing.T) {
	q := createQueues(t, 5, 5*time.Second)
	first := startInstance(t, "sqs-term", baseEnv(q))
	const total = 40
	ws := make([]wallet, 4)
	for i := range ws {
		ws[i] = openWallet(t, first, "100.00")
	}
	for i := range total {
		w := ws[i%len(ws)]
		ext := "term-" + uuid.NewString()
		sendSQS(t, q.WagerURL, sqsMessage("msg-"+uuid.NewString(), w, "provider-a", ext, "BET", "1.00", "", "k-"+ext), w.ID)
	}
	time.Sleep(300 * time.Millisecond)
	if err := first.Terminate(20 * time.Second); err != nil {
		t.Fatalf("encerramento gracioso falhou: %v\n%s", err, tail(first.Logs(), 30))
	}
	if !containsAll(first.Logs(), "worker encerrado", "sqs-consumer", "pool do PostgreSQL fechado") {
		t.Fatal("logs de encerramento ordenado ausentes")
	}

	second := startInstance(t, "sqs-after", baseEnv(q))
	eventually(t, 40*time.Second, "todas as mensagens processadas", func() bool {
		v, f := queueDepth(t, q.WagerURL)
		return v == 0 && f == 0
	})
	_ = second
	var debits int64
	for _, w := range ws {
		debits += queryInt(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.ID)
		assertLedgerConsistent(t, w.ID)
	}
	if debits != total {
		t.Fatalf("débitos = %d, esperado %d", debits, total)
	}
}
