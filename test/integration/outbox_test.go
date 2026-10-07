//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/uuid"
)

// publisherOnly configura uma instância que apenas publica a outbox.
func publisherOnly(q queues) map[string]string {
	e := baseEnv(q)
	e["SQS_CONSUMER_ENABLED"] = "false"
	e["PENDING_WORKER_ENABLED"] = "false"
	return e
}

// apiOnly configura uma instância sem publicador (eventos ficam na outbox).
func apiOnly(q queues) map[string]string {
	e := baseEnv(q)
	e["OUTBOX_PUBLISHER_ENABLED"] = "false"
	return e
}

func walletEventIDs(t *testing.T, walletIDs []string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, w := range walletIDs {
		rows, err := adminPool.Query(t.Context(), `SELECT event_id::text FROM outbox_events WHERE partition_key = $1`, w)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id string
			_ = rows.Scan(&id)
			out[id] = true
		}
		rows.Close()
	}
	return out
}

func unpublished(t *testing.T, ids map[string]bool) int {
	n := 0
	for id := range ids {
		n += int(queryInt(t, `SELECT COUNT(*) FROM outbox_events WHERE event_id = $1 AND published_at IS NULL`, id))
	}
	return n
}

type receivedEvent struct {
	EventID   string          `json:"eventId"`
	EventType string          `json:"eventType"`
	Version   int             `json:"version"`
	Data      json.RawMessage `json:"data"`
}

func receivedEvents(t *testing.T, q queues) map[string][]receivedEvent {
	t.Helper()
	out := map[string][]receivedEvent{}
	for _, m := range drainQueue(t, q.EventsURL, 2*time.Second) {
		var ev receivedEvent
		if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &ev); err != nil {
			t.Fatalf("evento ilegível: %v", err)
		}
		if aws.ToString(m.MessageAttributes["eventId"].StringValue) != ev.EventID {
			t.Fatal("atributo eventId difere do payload")
		}
		out[ev.EventID] = append(out[ev.EventID], ev)
	}
	return out
}

func TestOutboxCompetingPublishers(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	api := startInstance(t, "ob-api", apiOnly(q))
	var walletIDs []string
	for range 20 {
		w := openWallet(t, api, "10.00")
		walletIDs = append(walletIDs, w.ID)
		ext := "ob-" + uuid.NewString()
		submit(t, api, "provider-a", wagerBody(w, "provider-a", ext, "BET", "1.00", ""), "provider-a:"+ext)
	}
	ids := walletEventIDs(t, walletIDs)
	if len(ids) != 80 || unpublished(t, ids) != 80 {
		t.Fatalf("eventos esperados na outbox = 80, obtidos %d (pendentes %d)", len(ids), unpublished(t, ids))
	}

	e1, e2 := publisherOnly(q), publisherOnly(q)
	e1["OUTBOX_BATCH_SIZE"], e2["OUTBOX_BATCH_SIZE"] = "3", "3"
	p1 := startInstance(t, "ob-pub-1", e1)
	p2 := startInstance(t, "ob-pub-2", e2)
	eventually(t, 30*time.Second, "outbox totalmente publicada", func() bool { return unpublished(t, ids) == 0 })

	got := receivedEvents(t, q)
	for id := range ids {
		if len(got[id]) != 1 {
			t.Fatalf("evento %s recebido %d vezes", id, len(got[id]))
		}
	}
	c1, c2 := p1.Metric("outbox_published_total"), p2.Metric("outbox_published_total")
	if c1+c2 < 80 || c1 == 0 || c2 == 0 {
		t.Fatalf("os dois publicadores devem disputar a outbox: %v e %v", c1, c2)
	}
	// Contrato do envelope.
	for _, evs := range got {
		ev := evs[0]
		if ev.Version != 1 || ev.EventType == "" || len(ev.Data) == 0 {
			t.Fatalf("envelope inválido: %+v", ev)
		}
	}
}

// Interrupção entre o COMMIT e a publicação: a instância cai após reivindicar
// o lote; outra instância assume após o lease e publica.
func TestOutboxCrashBetweenCommitAndPublish(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	api := startInstance(t, "obc-api", apiOnly(q))
	w := openWallet(t, api, "50.00")
	ids := walletEventIDs(t, []string{w.ID})

	e := publisherOnly(q)
	e["FAULT_CRASH_POINTS"] = "outbox.before_publish"
	crashing := launch(t, "obc-crash", e)
	if !crashing.WaitExit(15 * time.Second) {
		t.Fatal("publicador deveria cair antes de publicar")
	}
	if unpublished(t, ids) != len(ids) {
		t.Fatal("nenhum evento deveria ter sido confirmado")
	}
	startInstance(t, "obc-takeover", publisherOnly(q))
	eventually(t, 20*time.Second, "eventos publicados pela outra instância", func() bool { return unpublished(t, ids) == 0 })
	got := receivedEvents(t, q)
	for id := range ids {
		if len(got[id]) != 1 {
			t.Fatalf("evento %s recebido %d vezes", id, len(got[id]))
		}
	}
}

// Interrupção entre a publicação e a confirmação na outbox: o evento é
// republicado por outra instância com o MESMO eventId e payload idêntico.
func TestOutboxCrashBetweenPublishAndConfirm(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	api := startInstance(t, "obp-api", apiOnly(q))
	w := openWallet(t, api, "50.00")
	ids := walletEventIDs(t, []string{w.ID})

	e := publisherOnly(q)
	e["FAULT_CRASH_POINTS"] = "outbox.after_publish_before_mark"
	e["OUTBOX_BATCH_SIZE"] = "1"
	crashing := launch(t, "obp-crash", e)
	if !crashing.WaitExit(15 * time.Second) {
		t.Fatal("publicador deveria cair após publicar")
	}
	firstCopy := drainQueue(t, q.EventsURL, 2*time.Second)
	if len(firstCopy) != 1 {
		t.Fatalf("esperada 1 publicação antes da queda, obtidas %d", len(firstCopy))
	}
	var first receivedEvent
	_ = json.Unmarshal([]byte(aws.ToString(firstCopy[0].Body)), &first)
	if queryInt(t, `SELECT COUNT(*) FROM outbox_events WHERE event_id = $1 AND published_at IS NULL`, first.EventID) != 1 {
		t.Fatal("o evento publicado não pode estar confirmado (queda antes da confirmação)")
	}

	// O destino deduplica por MessageDeduplicationId (=eventId) por 5 minutos;
	// para observar a republicação, a segunda instância usa uma fila nova com o
	// mesmo contrato (simula o término da janela de deduplicação).
	q2 := createQueues(t, 3, 5*time.Second)
	e2 := publisherOnly(q)
	e2["SQS_EVENTS_QUEUE_NAME"] = q2.Events
	startInstance(t, "obp-takeover", e2)
	eventually(t, 20*time.Second, "republicação confirmada", func() bool { return unpublished(t, ids) == 0 })
	again := receivedEvents(t, q2)
	if len(again[first.EventID]) != 1 {
		t.Fatalf("republicação do evento %s: %d cópias", first.EventID, len(again[first.EventID]))
	}
	if attempts := queryInt(t, `SELECT attempts FROM outbox_events WHERE event_id = $1`, first.EventID); attempts < 2 {
		t.Fatalf("tentativas = %d, esperado >= 2", attempts)
	}
	// O payload republicado é byte a byte o snapshot gravado na outbox.
	var stored string
	_ = adminPool.QueryRow(t.Context(), `SELECT payload::text FROM outbox_events WHERE event_id = $1`, first.EventID).Scan(&stored)
	secondCopy := again[first.EventID][0]
	if aws.ToString(firstCopy[0].Body) != stored || secondCopy.EventType != first.EventType || string(secondCopy.Data) != string(first.Data) {
		t.Fatalf("republicação deve preservar o snapshot:\n%s\n%s", aws.ToString(firstCopy[0].Body), stored)
	}
}

// Indisponibilidade do destino: publicação falha, é reagendada com backoff e
// concluída quando o destino volta.
func TestOutboxRetryWithBackoff(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	api := startInstance(t, "obr-api", apiOnly(q))
	w := openWallet(t, api, "50.00")
	ids := walletEventIDs(t, []string{w.ID})

	e := publisherOnly(q)
	e["FAULT_ERROR_POINTS"] = "outbox.publish"
	failing := startInstance(t, "obr-failing", e)
	eventually(t, 15*time.Second, "falhas registradas com backoff", func() bool {
		return queryInt(t, fmt.Sprintf(`SELECT COUNT(*) FROM outbox_events WHERE partition_key = '%s' AND attempts >= 2 AND last_error IS NOT NULL AND published_at IS NULL`, w.ID)) == int64(len(ids))
	})
	if failing.Metric("outbox_publish_failures_total") < 2 {
		t.Fatal("falhas de publicação deveriam ser contabilizadas")
	}
	if err := failing.Terminate(15 * time.Second); err != nil {
		t.Fatal(err)
	}
	startInstance(t, "obr-healthy", publisherOnly(q))
	eventually(t, 20*time.Second, "eventos publicados após recuperação", func() bool { return unpublished(t, ids) == 0 })
	got := receivedEvents(t, q)
	for id := range ids {
		if len(got[id]) != 1 {
			t.Fatalf("evento %s recebido %d vezes", id, len(got[id]))
		}
	}
}
