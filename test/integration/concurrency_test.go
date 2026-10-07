//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// startCluster inicia n instâncias independentes compartilhando banco e filas.
func startCluster(t *testing.T, n int, q queues, override map[string]string) []*instance {
	t.Helper()
	out := make([]*instance, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := baseEnv(q)
			for k, v := range override {
				e[k] = v
			}
			out[i] = launch(t, fmt.Sprintf("node-%d-%s", i+1, uuid.NewString()[:4]), e)
		}()
	}
	wg.Wait()
	for _, inst := range out {
		inst.waitReady(30 * time.Second)
	}
	return out
}

func TestOpenWalletAtomicAndUnique(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	inst := startInstance(t, "open-1", baseEnv(q))

	w := openWallet(t, inst, "1000.00")
	if n := queryInt(t, `SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING' AND status = 'PROCESSED' AND origin = 'INTERNAL'`, w.ID); n != 1 {
		t.Fatalf("OPENING = %d", n)
	}
	if n := queryInt(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'CREDIT' AND wallet_version = 1`, w.ID); n != 1 {
		t.Fatalf("lançamento de abertura = %d", n)
	}
	if n := queryInt(t, `SELECT COUNT(*) FROM outbox_events WHERE partition_key = $1 AND event_type IN ('WagerTransactionProcessed','WalletBalanceChanged')`, w.ID); n != 2 {
		t.Fatalf("eventos da abertura = %d", n)
	}
	if v := queryInt(t, `SELECT version FROM wallets WHERE id = $1`, w.ID); v != 1 {
		t.Fatalf("versão = %d", v)
	}

	z := openWallet(t, inst, "0.00")
	if n := queryInt(t, `SELECT (SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1) + (SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1) + (SELECT COUNT(*) FROM outbox_events WHERE partition_key = $1::text)`, z.ID); n != 0 {
		t.Fatalf("saldo zero não deve criar OPENING/ledger/eventos (%d registros)", n)
	}

	dup := call(t, "POST", inst.Base+"/wallets", token(t, "wallet-service"), map[string]any{
		"playerId": w.PlayerID, "initialBalance": map[string]string{"amount": "5.00", "currency": "BRL"},
	}, nil)
	if dup.Status != http.StatusConflict || dup.str("error", "code") != "WALLET_ALREADY_EXISTS" {
		t.Fatalf("duplicada: %d %s", dup.Status, dup.Raw)
	}
}

// Teste obrigatório: carteira com 100.00 recebe simultaneamente duas apostas
// distintas de 80.00, em instâncias diferentes.
func TestTwoBetsOf80AcrossThreeInstances(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	nodes := startCluster(t, 3, q, nil)

	for round := range 5 {
		w := openWallet(t, nodes[0], "100.00")
		exts := []string{fmt.Sprintf("bet80-a-%d-%s", round, uuid.NewString()), fmt.Sprintf("bet80-b-%d-%s", round, uuid.NewString())}
		results := make([]response, 2)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				results[i] = submit(t, nodes[i], "provider-a", wagerBody(w, "provider-a", exts[i], "BET", "80.00", ""), "provider-a:"+exts[i])
			}()
		}
		close(start)
		wg.Wait()

		processed, rejected := -1, -1
		for i, r := range results {
			switch {
			case r.Status == http.StatusCreated && r.str("status") == "PROCESSED":
				processed = i
			case r.Status == http.StatusUnprocessableEntity && r.str("failureCode") == "INSUFFICIENT_FUNDS":
				rejected = i
			default:
				t.Fatalf("rodada %d: resposta inesperada %d %s", round, r.Status, r.Raw)
			}
		}
		if processed < 0 || rejected < 0 {
			t.Fatalf("rodada %d: esperado 1 processada e 1 rejeitada", round)
		}
		if got := results[processed].str("balance", "amount"); got != "20.00" {
			t.Fatalf("saldo da aposta processada = %s", got)
		}
		if bal := walletBalance(t, w.ID); bal != 2000 {
			t.Fatalf("saldo final = %d", bal)
		}
		if n := queryInt(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.ID); n != 1 {
			t.Fatalf("débitos = %d", n)
		}

		// Reenvios (por outra instância) não alteram o resultado.
		for i := range 2 {
			r := submit(t, nodes[2], "provider-a", wagerBody(w, "provider-a", exts[i], "BET", "80.00", ""), "provider-a:"+exts[i])
			if r.Body["idempotentReplay"] != true || r.str("transactionId") != results[i].str("transactionId") || r.str("status") != results[i].str("status") {
				t.Fatalf("reenvio %d: %s", i, r.Raw)
			}
		}
		if bal := walletBalance(t, w.ID); bal != 2000 {
			t.Fatalf("saldo após reenvios = %d", bal)
		}
		assertLedgerConsistent(t, w.ID)
	}
}

// A mesma aposta enviada 50 vezes em paralelo (HTTP em 3 instâncias) e também
// por SQS com mensagens distintas resulta em um único débito.
func TestSameBetFiftyTimesAcrossHTTPAndSQS(t *testing.T) {
	q := createQueues(t, 5, 5*time.Second)
	nodes := startCluster(t, 3, q, nil)
	w := openWallet(t, nodes[0], "100.00")
	ext := "dup-" + uuid.NewString()
	key := "provider-a:" + ext
	body := wagerBody(w, "provider-a", ext, "BET", "10.00", "")

	before := 0.0
	for _, n := range nodes {
		before += n.Metric("wager_duplicates_total")
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	fresh, replays := 0, 0
	ids := map[string]bool{}
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r := submit(t, nodes[i%3], "provider-a", body, key)
			mu.Lock()
			defer mu.Unlock()
			if r.Status != http.StatusCreated && r.Status != http.StatusOK {
				t.Errorf("status %d %s", r.Status, r.Raw)
				return
			}
			ids[r.str("transactionId")] = true
			if r.Body["idempotentReplay"] == true {
				replays++
			} else {
				fresh++
			}
		}()
	}
	// Em paralelo, 10 mensagens SQS distintas com a mesma chave de idempotência.
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			sendSQS(t, q.WagerURL, sqsMessage(fmt.Sprintf("dup-msg-%d-%s", i, ext), w, "provider-a", ext, "BET", "10.00", "", key), w.ID)
		}()
	}
	close(start)
	wg.Wait()

	eventually(t, 30*time.Second, "fila de entrada vazia", func() bool {
		v, f := queueDepth(t, q.WagerURL)
		return v == 0 && f == 0
	})
	if fresh > 1 || len(ids) != 1 {
		t.Fatalf("aplicações efetivas via HTTP = %d, ids distintos = %d", fresh, len(ids))
	}
	if n := queryInt(t, `SELECT COUNT(*) FROM wager_transactions WHERE provider_id = 'provider-a' AND external_transaction_id = $1`, ext); n != 1 {
		t.Fatalf("transações = %d", n)
	}
	if n := queryInt(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, w.ID); n != 1 {
		t.Fatalf("débitos = %d", n)
	}
	if bal := walletBalance(t, w.ID); bal != 9000 {
		t.Fatalf("saldo = %d", bal)
	}
	if n := queryInt(t, `SELECT COUNT(*) FROM inbox_messages WHERE message_id LIKE $1`, "dup-msg-%-"+ext); n != 10 {
		t.Fatalf("mensagens registradas na inbox = %d", n)
	}
	// Recebimentos repetidos comprovados pela deduplicação da aplicação: 59 dos 60.
	after := 0.0
	for _, n := range nodes {
		after += n.Metric("wager_duplicates_total")
	}
	if after-before != 59 {
		t.Fatalf("duplicatas contabilizadas = %v, esperado 59 (replays HTTP=%d)", after-before, replays)
	}
	assertLedgerConsistent(t, w.ID)
}

// Carteiras distintas avançam em paralelo; o lock de uma não bloqueia outra.
func TestDistinctWalletsProgressInParallel(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	nodes := startCluster(t, 3, q, nil)

	const wallets, bets = 15, 8
	ws := make([]wallet, wallets)
	for i := range ws {
		ws[i] = openWallet(t, nodes[i%3], "100.00")
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, w := range ws {
		for j := range bets {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				ext := fmt.Sprintf("par-%d-%d-%s", i, j, uuid.NewString())
				r := submit(t, nodes[(i+j)%3], "provider-a", wagerBody(w, "provider-a", ext, "BET", "1.25", ""), "provider-a:"+ext)
				if r.Status != http.StatusCreated {
					t.Errorf("aposta: %d %s", r.Status, r.Raw)
				}
			}()
		}
	}
	close(start)
	wg.Wait()
	for _, w := range ws {
		if bal := walletBalance(t, w.ID); bal != 10000-bets*125 {
			t.Fatalf("carteira %s saldo %d", w.ID, bal)
		}
		assertLedgerConsistent(t, w.ID)
		r := call(t, "POST", nodes[0].Base+"/wallets/"+w.ID+"/reconciliation", token(t, "wallet-service"), nil, nil)
		if r.Body["consistent"] != true {
			t.Fatalf("reconciliação: %s", r.Raw)
		}
	}

	// Independência de locks: segura o lock da carteira A numa transação externa;
	// a carteira B continua sendo processada enquanto A aguarda.
	a, b := ws[0], ws[1]
	ctx := context.Background()
	holder, err := adminPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, a.ID); err != nil {
		t.Fatal(err)
	}
	doneA := make(chan response, 1)
	go func() {
		ext := "lock-a-" + uuid.NewString()
		doneA <- submit(t, nodes[0], "provider-a", wagerBody(a, "provider-a", ext, "BET", "1.00", ""), "provider-a:"+ext)
	}()
	time.Sleep(300 * time.Millisecond)
	startB := time.Now()
	extB := "lock-b-" + uuid.NewString()
	rb := submit(t, nodes[1], "provider-a", wagerBody(b, "provider-a", extB, "BET", "1.00", ""), "provider-a:"+extB)
	if rb.Status != http.StatusCreated || time.Since(startB) > 2*time.Second {
		t.Fatalf("carteira B deveria ser processada sem esperar A: %d em %s", rb.Status, time.Since(startB))
	}
	select {
	case r := <-doneA:
		t.Fatalf("carteira A deveria aguardar o lock: %d %s", r.Status, r.Raw)
	default:
	}
	if err := holder.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
		t.Fatal(err)
	}
	if r := <-doneA; r.Status != http.StatusCreated {
		t.Fatalf("carteira A após liberar o lock: %d %s", r.Status, r.Raw)
	}
}

// A reconciliação reporta divergências (resposta, log e métrica) sem alterar o saldo.
func TestReconciliationReportsDivergence(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	inst := startInstance(t, "recon-1", baseEnv(q))
	w := openWallet(t, inst, "1000.00")
	ext := "rc-" + uuid.NewString()
	if r := submit(t, inst, "provider-a", wagerBody(w, "provider-a", ext, "BET", "25.00", ""), "provider-a:"+ext); r.Status != http.StatusCreated {
		t.Fatal(r.Raw)
	}
	r := call(t, "POST", inst.Base+"/wallets/"+w.ID+"/reconciliation", token(t, "wallet-service"), nil, nil)
	if r.Body["consistent"] != true || r.str("storedBalance", "amount") != "975.00" || r.str("difference", "amount") != "0.00" || r.Body["checkedEntries"] != float64(2) {
		t.Fatalf("reconciliação consistente: %s", r.Raw)
	}

	// Simula corrupção externa desligando triggers como superusuário.
	ctx := context.Background()
	conn, err := adminPool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, `SET session_replication_role = replica; UPDATE wallets SET balance_minor = balance_minor + 500 WHERE id = '`+w.ID+`'; SET session_replication_role = DEFAULT;`)
	conn.Release()
	if err != nil {
		t.Fatal(err)
	}
	before := inst.Metric("wallet_reconciliation_divergences_total")
	r = call(t, "POST", inst.Base+"/wallets/"+w.ID+"/reconciliation", token(t, "wallet-service"), nil, nil)
	if r.Body["consistent"] != false || r.str("difference", "amount") != "5.00" || r.str("calculatedBalance", "amount") != "975.00" {
		t.Fatalf("divergência: %s", r.Raw)
	}
	if inst.Metric("wallet_reconciliation_divergences_total")-before != 1 {
		t.Fatal("métrica de divergência não incrementada")
	}
	eventually(t, 2*time.Second, "log de divergência", func() bool {
		return containsAll(inst.Logs(), "divergência de reconciliação", w.ID)
	})
	if walletBalance(t, w.ID) != 98000 {
		t.Fatal("a reconciliação não pode alterar o saldo")
	}
}
