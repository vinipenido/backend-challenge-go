package sqs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.uber.org/fx"

	"github.com/vinipenido/backend-challenge-go/internal/app"
	"github.com/vinipenido/backend-challenge-go/internal/config"
	"github.com/vinipenido/backend-challenge-go/internal/platform/fault"
	"github.com/vinipenido/backend-challenge-go/internal/platform/metrics"
	"github.com/vinipenido/backend-challenge-go/internal/platform/worker"
)

// MessageType é o único tipo de mensagem aceito na fila de entrada.
const MessageType = "WagerTransactionRequested"

// Envelope é o corpo das mensagens da fila wager-transactions.fifo.
type Envelope struct {
	MessageID     string       `json:"messageId"`
	Type          string       `json:"type"`
	OccurredAt    string       `json:"occurredAt"`
	CorrelationID string       `json:"correlationId,omitempty"`
	Data          *MessageData `json:"data"`
}

// MessageData são os campos de negócio (iguais ao corpo HTTP) mais a chave.
type MessageData struct {
	app.WagerRequestInput
	IdempotencyKey string `json:"idempotencyKey"`
}

// errPermanent marca mensagens que nunca terão sucesso em nova tentativa.
type errPermanent struct {
	reason string
	err    error
}

func (e *errPermanent) Error() string { return e.reason + ": " + e.err.Error() }

func permanent(reason string, err error) error { return &errPermanent{reason: reason, err: err} }

// ParseEnvelope valida o envelope de forma estrita (campos desconhecidos são rejeitados).
func ParseEnvelope(body []byte) (Envelope, error) {
	var env Envelope
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		return Envelope{}, fmt.Errorf("JSON inválido: %w", err)
	}
	if env.MessageID == "" || len(env.MessageID) > 128 {
		return Envelope{}, errors.New("messageId obrigatório (até 128 caracteres)")
	}
	if env.Type != MessageType {
		return Envelope{}, fmt.Errorf("type %q não suportado", env.Type)
	}
	if _, err := time.Parse(time.RFC3339Nano, env.OccurredAt); err != nil {
		return Envelope{}, errors.New("occurredAt deve estar em RFC 3339")
	}
	if env.Data == nil {
		return Envelope{}, errors.New("data obrigatório")
	}
	return env, nil
}

// Consumer consome wager-transactions.fifo e delega ao mesmo caso de uso do HTTP.
type Consumer struct {
	cfg      config.Config
	log      *slog.Logger
	metrics  *metrics.Metrics
	client   *sqs.Client
	queues   *Queues
	wagering *app.WageringService
	clock    app.Clock
	trusted  map[string]struct{}
	runner   *worker.Runner
}

// ConsumerParams agrupa as dependências do consumidor.
type ConsumerParams struct {
	fx.In
	Lifecycle fx.Lifecycle
	Config    config.Config
	Log       *slog.Logger
	Metrics   *metrics.Metrics
	Client    *sqs.Client
	Queues    *Queues
	Wagering  *app.WageringService
	Clock     app.Clock
}

// NewConsumer cria o consumidor e registra seu ciclo de vida.
func NewConsumer(p ConsumerParams) *Consumer {
	c := &Consumer{
		cfg: p.Config, log: p.Log.With(slog.String("component", "sqs-consumer")), metrics: p.Metrics,
		client: p.Client, queues: p.Queues, wagering: p.Wagering, clock: p.Clock,
		trusted: map[string]struct{}{},
	}
	for _, id := range p.Config.SQSTrustedProviders {
		c.trusted[id] = struct{}{}
	}
	c.runner = worker.NewRunner("sqs-consumer", c.log)
	if !p.Config.SQSConsumerEnabled {
		return c
	}
	p.Lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			c.runner.Start(p.Config.SQSPollers, c.poll)
			return nil
		},
		OnStop: c.runner.Stop,
	})
	return c
}

// Runner expõe o executor (verificação de término nos testes).
func (c *Consumer) Runner() *worker.Runner { return c.runner }

// poll executa long polling até o cancelamento. Ao parar, nenhuma nova busca é
// feita; o lote em andamento é concluído ou devolvido à fila.
func (c *Consumer) poll(ctx context.Context, _ int) {
	failures := 0
	for ctx.Err() == nil {
		out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.queues.Wager()),
			MaxNumberOfMessages: int32(c.cfg.SQSMaxMessages),
			WaitTimeSeconds:     int32(c.cfg.SQSWaitTime / time.Second),
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
				types.MessageSystemAttributeNameMessageGroupId,
				types.MessageSystemAttributeNameSenderId,
			},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failures++
			c.metrics.SQSMessages.WithLabelValues("receive_error").Inc()
			c.log.Warn("falha ao receber mensagens; nova tentativa com backoff", slog.String("error", err.Error()))
			sleep(ctx, min(time.Duration(1<<min(failures, 5))*200*time.Millisecond, 10*time.Second))
			continue
		}
		failures = 0
		c.handleBatch(ctx, out.Messages)
	}
}

// handleBatch processa as mensagens agrupadas por MessageGroupId: sequencial
// dentro do grupo (ordem FIFO por carteira) e paralelo entre grupos.
func (c *Consumer) handleBatch(ctx context.Context, msgs []types.Message) {
	groups := map[string][]types.Message{}
	var order []string
	for _, m := range msgs {
		g := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
		if _, ok := groups[g]; !ok {
			order = append(order, g)
		}
		groups[g] = append(groups[g], m)
	}
	var wg sync.WaitGroup
	for _, g := range order {
		wg.Add(1)
		go func(batch []types.Message) {
			defer wg.Done()
			for i, m := range batch {
				if ctx.Err() != nil {
					// Encerramento: devolve imediatamente as mensagens não iniciadas.
					c.release(batch[i:])
					return
				}
				c.handle(m)
			}
		}(groups[g])
	}
	wg.Wait()
}

// handle processa uma mensagem com prazo próprio, independente do contexto de
// parada, para que o trabalho iniciado termine (ou expire) de forma controlada.
func (c *Consumer) handle(m types.Message) {
	c.metrics.SQSInFlight.Inc()
	defer c.metrics.SQSInFlight.Dec()
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.SQSProcessingTimeout)
	defer cancel()

	receiveCount, _ := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	log := c.log.With(slog.String("sqsMessageId", aws.ToString(m.MessageId)), slog.Int("receiveCount", receiveCount))

	err := c.process(ctx, m, log)
	var perm *errPermanent
	switch {
	case err == nil:
		// Commit confirmado: só agora a mensagem pode ser removida.
		fault.Crash("sqs.after_commit_before_delete")
		if derr := c.delete(m); derr != nil {
			// A reentrega será tratada como duplicata pela inbox.
			log.Warn("falha ao remover mensagem após commit", slog.String("error", derr.Error()))
		}
	case errors.As(err, &perm):
		log.Error("mensagem inválida ou conflitante; encaminhando à DLQ", slog.String("reason", perm.reason), slog.String("error", perm.err.Error()))
		c.toDLQ(m, perm.reason, log)
	default:
		// Falha transitória: mantém a mensagem e adia a próxima entrega
		// (backoff exponencial); após maxReceiveCount o redrive leva à DLQ.
		delay := min(c.cfg.SQSRetryBase<<min(max(receiveCount-1, 0), 10), c.cfg.SQSRetryMax)
		c.metrics.SQSMessages.WithLabelValues("retry").Inc()
		c.wageringRetryMetric()
		log.Warn("falha transitória; mensagem será reentregue", slog.String("error", err.Error()), slog.Duration("delay", delay))
		c.changeVisibility(m, delay)
	}
}

func (c *Consumer) wageringRetryMetric() { c.metrics.Retry("sqs_message") }

func (c *Consumer) process(ctx context.Context, m types.Message, log *slog.Logger) error {
	body := []byte(aws.ToString(m.Body))
	env, err := ParseEnvelope(body)
	if err != nil {
		return permanent("invalid_message", err)
	}
	*log = *log.With(slog.String("messageId", env.MessageID), slog.String("providerId", env.Data.ProviderID))
	if _, ok := c.trusted[env.Data.ProviderID]; !ok {
		return permanent("untrusted_provider", app.ErrUntrustedProvider)
	}
	hash, err := app.CanonicalJSONHash(body)
	if err != nil {
		return permanent("invalid_message", err)
	}
	req, err := app.ParseWagerRequest(env.Data.WagerRequestInput, env.Data.IdempotencyKey)
	if err != nil {
		return permanent("invalid_payload", err)
	}
	if err := fault.Error("sqs.before_process"); err != nil {
		return app.Transient(err)
	}
	correlation := env.CorrelationID
	if correlation == "" {
		correlation = env.MessageID
	}
	res, err := c.wagering.Process(ctx, app.ProcessCommand{
		Request: req, Channel: app.ChannelSQS, CorrelationID: correlation, CausationID: env.MessageID,
		Inbox: &app.InboxMessage{
			Consumer: c.cfg.SQSConsumerName, MessageID: env.MessageID, Hash: hash, ReceivedAt: c.clock.Now(),
		},
	})
	switch {
	case err == nil:
		outcome := string(res.Transaction.Status)
		if res.DuplicateMessage {
			outcome = "duplicate"
		}
		c.metrics.SQSMessages.WithLabelValues(outcome).Inc()
		log.Info("mensagem processada", slog.String("transactionId", res.Transaction.ID.String()),
			slog.String("status", string(res.Transaction.Status)), slog.Bool("duplicate", res.DuplicateMessage),
			slog.Bool("replay", res.Replay))
		return nil
	case app.IsTransient(err):
		return err
	case errors.Is(err, app.ErrPermanentFailure):
		return permanent("permanent_failure", err)
	case errors.Is(err, app.ErrInboxHashConflict):
		return permanent("message_hash_conflict", err)
	case errors.Is(err, app.ErrIdempotencyConflict), errors.Is(err, app.ErrExternalTransactionExists):
		return permanent("idempotency_conflict", err)
	case errors.Is(err, app.ErrWalletNotFound):
		return permanent("wallet_not_found", err)
	default:
		var ie interface{ Code() string }
		if errors.As(err, &ie) {
			return permanent("invalid_payload", err)
		}
		return err
	}
}

func (c *Consumer) delete(m types.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl: aws.String(c.queues.Wager()), ReceiptHandle: m.ReceiptHandle,
	})
	return err
}

func (c *Consumer) changeVisibility(m types.Message, d time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.queues.Wager()), ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: int32(d / time.Second),
	})
	if err != nil {
		c.log.Warn("falha ao alterar visibilidade (o visibility timeout padrão será aplicado)", slog.String("error", err.Error()))
	}
}

// release devolve mensagens não processadas para reentrega imediata.
func (c *Consumer) release(msgs []types.Message) {
	for _, m := range msgs {
		c.changeVisibility(m, 0)
	}
	if len(msgs) > 0 {
		c.metrics.SQSMessages.WithLabelValues("released").Add(float64(len(msgs)))
		c.log.Info("mensagens devolvidas à fila no encerramento", slog.Int("count", len(msgs)))
	}
}

// toDLQ envia a mensagem à DLQ com o motivo e só então a remove da fila de
// entrada. A deduplicação usa o MessageId do SQS, evitando cópias se houver
// queda entre o envio e a remoção.
func (c *Consumer) toDLQ(m types.Message, reason string, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	group := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	if group == "" {
		group = "invalid"
	}
	sum := sha256.Sum256([]byte(aws.ToString(m.MessageId)))
	_, err := c.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(c.queues.DLQ()),
		MessageBody:            m.Body,
		MessageGroupId:         aws.String(group),
		MessageDeduplicationId: aws.String(hex.EncodeToString(sum[:])),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"dlqReason":         {DataType: aws.String("String"), StringValue: aws.String(reason)},
			"originalMessageId": {DataType: aws.String("String"), StringValue: m.MessageId},
		},
	})
	if err != nil {
		log.Warn("falha ao enviar à DLQ; a mensagem será reentregue", slog.String("error", err.Error()))
		return
	}
	c.metrics.SQSDLQ.WithLabelValues(reason).Inc()
	c.metrics.SQSMessages.WithLabelValues("dlq").Inc()
	if err := c.delete(m); err != nil {
		log.Warn("falha ao remover mensagem enviada à DLQ", slog.String("error", err.Error()))
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
