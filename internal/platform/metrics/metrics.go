// Package metrics expõe métricas Prometheus e implementa a porta app.Metrics.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics agrupa todos os coletores do serviço.
type Metrics struct {
	registry *prometheus.Registry

	transactions        *prometheus.CounterVec
	processing          *prometheus.HistogramVec
	duplicates          *prometheus.CounterVec
	idempotencyConflict *prometheus.CounterVec
	concurrency         *prometheus.CounterVec
	retries             *prometheus.CounterVec
	reconciliations     *prometheus.CounterVec
	divergences         prometheus.Counter

	SQSMessages     *prometheus.CounterVec
	SQSDLQ          *prometheus.CounterVec
	SQSInFlight     prometheus.Gauge
	OutboxPublished prometheus.Counter
	OutboxFailures  prometheus.Counter
	OutboxPending   prometheus.Gauge
	OutboxOldestAge prometheus.Gauge
	OutboxDelay     prometheus.Histogram
	HTTPRequests    *prometheus.CounterVec
	HTTPDuration    *prometheus.HistogramVec
	AuthFailures    *prometheus.CounterVec
}

// New cria e registra os coletores.
func New() *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { r.MustRegister(c) }
	latency := []float64{.002, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

	m := &Metrics{
		registry: r,
		transactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_transactions_total", Help: "Resultados de operações por canal, tipo e status.",
		}, []string{"channel", "kind", "status", "replay"}),
		processing: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "wager_processing_duration_seconds", Help: "Latência de processamento das operações.", Buckets: latency,
		}, []string{"channel", "kind", "status"}),
		duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_duplicates_total", Help: "Recebimentos repetidos detectados (replay idempotente ou inbox).",
		}, []string{"channel", "reason"}),
		idempotencyConflict: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_idempotency_conflicts_total", Help: "Chaves reutilizadas com conteúdo diferente ou operação com outra chave.",
		}, []string{"channel"}),
		concurrency: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_concurrency_conflicts_total", Help: "Conflitos de concorrência detectados pelo banco.",
		}, []string{"reason"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "retries_total", Help: "Novas tentativas por componente.",
		}, []string{"component"}),
		reconciliations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_reconciliations_total", Help: "Reconciliações executadas por resultado.",
		}, []string{"consistent"}),
		divergences: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_reconciliation_divergences_total", Help: "Divergências entre saldo armazenado e ledger.",
		}),
		SQSMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sqs_messages_total", Help: "Mensagens SQS por resultado.",
		}, []string{"outcome"}),
		SQSDLQ: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sqs_dlq_messages_total", Help: "Mensagens encaminhadas à DLQ por motivo.",
		}, []string{"reason"}),
		SQSInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sqs_messages_in_flight", Help: "Mensagens em processamento nesta instância.",
		}),
		OutboxPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_published_total", Help: "Eventos publicados a partir da outbox.",
		}),
		OutboxFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_publish_failures_total", Help: "Falhas de publicação (reagendadas com backoff).",
		}),
		OutboxPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_pending_events", Help: "Eventos ainda não publicados.",
		}),
		OutboxOldestAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_oldest_pending_age_seconds", Help: "Atraso da outbox: idade do evento pendente mais antigo.",
		}),
		OutboxDelay: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "outbox_publish_delay_seconds", Help: "Tempo entre a ocorrência do evento e sua publicação.",
			Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300},
		}),
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total", Help: "Requisições HTTP por rota e status.",
		}, []string{"route", "status"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "http_request_duration_seconds", Help: "Latência HTTP por rota.", Buckets: latency,
		}, []string{"route"}),
		AuthFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "auth_failures_total", Help: "Falhas de autenticação/autorização.",
		}, []string{"reason"}),
	}
	for _, c := range []prometheus.Collector{
		m.transactions, m.processing, m.duplicates, m.idempotencyConflict, m.concurrency, m.retries,
		m.reconciliations, m.divergences, m.SQSMessages, m.SQSDLQ, m.SQSInFlight, m.OutboxPublished,
		m.OutboxFailures, m.OutboxPending, m.OutboxOldestAge, m.OutboxDelay, m.HTTPRequests, m.HTTPDuration,
		m.AuthFailures,
	} {
		f(c)
	}
	return m
}

// Handler expõe /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// TransactionOutcome registra o resultado e a latência de uma operação.
func (m *Metrics) TransactionOutcome(channel, kind, status string, replay bool, d time.Duration) {
	r := "false"
	if replay {
		r = "true"
	}
	m.transactions.WithLabelValues(channel, kind, status, r).Inc()
	m.processing.WithLabelValues(channel, kind, status).Observe(d.Seconds())
}

// Duplicate registra um recebimento repetido.
func (m *Metrics) Duplicate(channel, reason string) {
	m.duplicates.WithLabelValues(channel, reason).Inc()
}

// IdempotencyConflict registra um conflito de chave.
func (m *Metrics) IdempotencyConflict(channel string) {
	m.idempotencyConflict.WithLabelValues(channel).Inc()
}

// ConcurrencyConflict registra um conflito de concorrência.
func (m *Metrics) ConcurrencyConflict(reason string) { m.concurrency.WithLabelValues(reason).Inc() }

// Retry registra uma nova tentativa.
func (m *Metrics) Retry(component string) { m.retries.WithLabelValues(component).Inc() }

// Reconciliation registra o resultado de uma reconciliação.
func (m *Metrics) Reconciliation(consistent bool) {
	if consistent {
		m.reconciliations.WithLabelValues("true").Inc()
		return
	}
	m.reconciliations.WithLabelValues("false").Inc()
	m.divergences.Inc()
}
