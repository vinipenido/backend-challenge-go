package sqs

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"go.uber.org/fx"

	"github.com/vinipenido/backend-challenge-go/internal/adapters/postgres"
	"github.com/vinipenido/backend-challenge-go/internal/config"
	"github.com/vinipenido/backend-challenge-go/internal/platform/fault"
	"github.com/vinipenido/backend-challenge-go/internal/platform/metrics"
	"github.com/vinipenido/backend-challenge-go/internal/platform/worker"
)

// Publisher publica os eventos da outbox em wallet-events.fifo.
//
// Contrato de roteamento: MessageGroupId = walletId (ordem por carteira),
// MessageDeduplicationId = eventId (deduplicação do SQS por 5 minutos) e
// atributos eventType, eventId, eventVersion e correlationId. A entrega é
// at-least-once: consumidores devem deduplicar por eventId.
type Publisher struct {
	cfg     config.Config
	log     *slog.Logger
	metrics *metrics.Metrics
	client  *sqs.Client
	queues  *Queues
	store   *postgres.OutboxStore
	owner   string
	runner  *worker.Runner
}

// PublisherParams agrupa as dependências do publicador.
type PublisherParams struct {
	fx.In
	Lifecycle fx.Lifecycle
	Config    config.Config
	Log       *slog.Logger
	Metrics   *metrics.Metrics
	Client    *sqs.Client
	Queues    *Queues
	Store     *postgres.OutboxStore
}

// NewPublisher cria o publicador e registra seu ciclo de vida.
func NewPublisher(p PublisherParams) *Publisher {
	pub := &Publisher{
		cfg: p.Config, log: p.Log.With(slog.String("component", "outbox-publisher")), metrics: p.Metrics,
		client: p.Client, queues: p.Queues, store: p.Store,
		// Identidade única por processo: um lease de uma execução anterior da
		// mesma instância nunca é confundido com o atual.
		owner: p.Config.InstanceID + "/" + uuid.NewString()[:8],
	}
	pub.runner = worker.NewRunner("outbox-publisher", pub.log)
	if !p.Config.OutboxEnabled {
		return pub
	}
	p.Lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			pub.runner.Start(2, func(ctx context.Context, id int) {
				if id == 0 {
					pub.loop(ctx)
				} else {
					pub.statsLoop(ctx)
				}
			})
			return nil
		},
		OnStop: func(ctx context.Context) error {
			err := pub.runner.Stop(ctx)
			// Libera leases não concluídos para que outra instância assuma já.
			if rerr := pub.store.Release(ctx, pub.owner); rerr != nil {
				pub.log.Warn("falha ao liberar leases da outbox", slog.String("error", rerr.Error()))
			}
			return err
		},
	})
	return pub
}

// Runner expõe o executor (verificação de término nos testes).
func (p *Publisher) Runner() *worker.Runner { return p.runner }

func (p *Publisher) loop(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := p.PublishBatch(ctx)
		if err != nil {
			p.log.Warn("falha ao reivindicar eventos da outbox", slog.String("error", err.Error()))
			sleep(ctx, 2*time.Second)
			continue
		}
		if n < p.cfg.OutboxBatchSize {
			sleep(ctx, p.cfg.OutboxPollInterval)
		}
	}
}

// PublishBatch reivindica e publica um lote; devolve quantos foram reivindicados.
func (p *Publisher) PublishBatch(ctx context.Context) (int, error) {
	recs, err := p.store.Claim(ctx, p.owner, p.cfg.OutboxLease, p.cfg.OutboxBatchSize)
	if err != nil {
		return 0, err
	}
	blocked := map[string]bool{}
	for _, rec := range recs {
		log := p.log.With(slog.String("eventId", rec.EventID.String()), slog.String("eventType", rec.EventType),
			slog.String("correlationId", rec.CorrelationID), slog.String("walletId", rec.PartitionKey))
		if blocked[rec.PartitionKey] {
			// Preserva a ordem da carteira: não publica eventos posteriores a uma falha.
			_ = p.store.MarkFailed(context.WithoutCancel(ctx), rec.EventID, p.owner, time.Now().Add(p.cfg.OutboxRetryBase), "evento anterior da carteira pendente")
			continue
		}
		fault.Crash("outbox.before_publish")
		if err := p.send(ctx, rec); err != nil {
			blocked[rec.PartitionKey] = true
			next := time.Now().Add(p.backoff(rec.Attempts))
			p.metrics.OutboxFailures.Inc()
			p.metrics.Retry("outbox_publish")
			log.Warn("falha ao publicar evento; nova tentativa agendada", slog.String("error", err.Error()), slog.Time("nextAttemptAt", next))
			_ = p.store.MarkFailed(context.WithoutCancel(ctx), rec.EventID, p.owner, next, err.Error())
			continue
		}
		fault.Crash("outbox.after_publish_before_mark")
		marked, err := p.store.MarkPublished(context.WithoutCancel(ctx), rec.EventID)
		if err != nil {
			// O lease expira e outra instância republica com o mesmo eventId.
			log.Warn("evento publicado, mas confirmação na outbox falhou", slog.String("error", err.Error()))
			continue
		}
		if marked {
			p.metrics.OutboxPublished.Inc()
			p.metrics.OutboxDelay.Observe(time.Since(rec.OccurredAt).Seconds())
			log.Debug("evento publicado", slog.Int("attempts", rec.Attempts))
		}
	}
	return len(recs), nil
}

func (p *Publisher) send(ctx context.Context, rec postgres.OutboxRecord) error {
	if err := fault.Error("outbox.publish"); err != nil {
		return err
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	attrs := map[string]types.MessageAttributeValue{
		"eventType":    {DataType: aws.String("String"), StringValue: aws.String(rec.EventType)},
		"eventId":      {DataType: aws.String("String"), StringValue: aws.String(rec.EventID.String())},
		"eventVersion": {DataType: aws.String("Number"), StringValue: aws.String(strconv.Itoa(rec.EventVersion))},
	}
	if rec.CorrelationID != "" {
		attrs["correlationId"] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(rec.CorrelationID)}
	}
	_, err := p.client.SendMessage(sctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(p.queues.Events()),
		MessageBody:            aws.String(rec.Payload),
		MessageGroupId:         aws.String(rec.PartitionKey),
		MessageDeduplicationId: aws.String(rec.EventID.String()),
		MessageAttributes:      attrs,
	})
	return err
}

func (p *Publisher) backoff(attempts int) time.Duration {
	d := p.cfg.OutboxRetryBase << min(max(attempts-1, 0), 16)
	return min(d, p.cfg.OutboxRetryMax)
}

// statsLoop atualiza periodicamente as métricas de atraso da outbox.
func (p *Publisher) statsLoop(ctx context.Context) {
	for ctx.Err() == nil {
		st, err := p.store.Stats(ctx)
		if err == nil {
			p.metrics.OutboxPending.Set(float64(st.Pending))
			p.metrics.OutboxOldestAge.Set(st.OldestAgeSeconds)
		}
		sleep(ctx, 5*time.Second)
	}
}
