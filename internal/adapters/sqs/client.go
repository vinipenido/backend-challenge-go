// Package sqs integra o AWS SQS: consumo de operações, encaminhamento para a
// DLQ e publicação dos eventos da outbox.
package sqs

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.uber.org/fx"

	"github.com/vinipenido/backend-challenge-go/internal/config"
	"github.com/vinipenido/backend-challenge-go/internal/platform/health"
	"github.com/vinipenido/backend-challenge-go/internal/platform/startup"
)

// NewClient cria o cliente SQS. As credenciais seguem a cadeia padrão da AWS
// (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, perfil, IAM role); AWS_ENDPOINT_URL
// aponta para o LocalStack no ambiente local.
func NewClient(cfg config.Config) (*sqs.Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(cfg.AWSRegion))
	if err != nil {
		return nil, fmt.Errorf("configuração AWS inválida: %w", err)
	}
	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.AWSEndpointURL != "" {
			o.BaseEndpoint = aws.String(cfg.AWSEndpointURL)
		}
	}), nil
}

// Queues contém as URLs das filas, resolvidas no início da aplicação.
type Queues struct {
	mu     sync.RWMutex
	wager  string
	dlq    string
	events string
}

// Wager devolve a URL da fila de entrada.
func (q *Queues) Wager() string { q.mu.RLock(); defer q.mu.RUnlock(); return q.wager }

// DLQ devolve a URL da DLQ de entrada.
func (q *Queues) DLQ() string { q.mu.RLock(); defer q.mu.RUnlock(); return q.dlq }

// Events devolve a URL da fila de eventos de saída.
func (q *Queues) Events() string { q.mu.RLock(); defer q.mu.RUnlock(); return q.events }

// NewQueues resolve as URLs no OnStart (com novas tentativas até o prazo de
// início), validando que as filas provisionadas existem.
func NewQueues(lc fx.Lifecycle, cfg config.Config, client *sqs.Client, log *slog.Logger) *Queues {
	q := &Queues{}
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		return startup.Retry(ctx, log, "sqs", func(ctx context.Context) error { return q.resolve(ctx, cfg, client) })
	}})
	return q
}

func (q *Queues) resolve(ctx context.Context, cfg config.Config, client *sqs.Client) error {
	get := func(name string) (string, error) {
		out, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
		if err != nil {
			return "", fmt.Errorf("fila %s: %w", name, err)
		}
		return aws.ToString(out.QueueUrl), nil
	}
	wager, err := get(cfg.SQSWagerQueueName)
	if err != nil {
		return err
	}
	dlq, err := get(cfg.SQSWagerDLQName)
	if err != nil {
		return err
	}
	events, err := get(cfg.SQSEventsQueueName)
	if err != nil {
		return err
	}
	q.mu.Lock()
	q.wager, q.dlq, q.events = wager, dlq, events
	q.mu.Unlock()
	return nil
}

// newReadiness verifica o SQS consultando atributos da fila de entrada.
func newReadiness(client *sqs.Client, q *Queues) health.Checker {
	return health.Checker{Name: "sqs", Check: func(ctx context.Context) error {
		if q.Wager() == "" {
			return fmt.Errorf("filas não resolvidas")
		}
		_, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(q.Wager()),
			AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
		})
		return err
	}}
}

// Module registra os componentes SQS no Fx.
var Module = fx.Module("sqs",
	fx.Provide(
		NewClient,
		NewQueues,
		health.AsChecker(newReadiness),
		NewConsumer,
		NewPublisher,
	),
	fx.Invoke(func(*Consumer, *Publisher) {}),
)
