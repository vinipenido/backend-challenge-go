// Package config carrega e valida a configuração a partir de variáveis de ambiente.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config contém toda a configuração da aplicação.
type Config struct {
	Env        string
	InstanceID string
	LogLevel   string

	HTTPEnabled        bool
	HTTPAddr           string
	HTTPRequestTimeout time.Duration

	DatabaseURL              string
	DatabaseMaxConns         int
	DatabaseLockTimeout      time.Duration
	DatabaseStatementTimeout time.Duration

	OIDCIssuerURL     string
	OIDCJWKSURL       string
	OIDCAudience      string
	OIDCProviderClaim string
	ProviderRole      string
	OperatorRole      string

	AWSRegion      string
	AWSEndpointURL string

	SQSWagerQueueName    string
	SQSWagerDLQName      string
	SQSEventsQueueName   string
	SQSConsumerEnabled   bool
	SQSConsumerName      string
	SQSPollers           int
	SQSWaitTime          time.Duration
	SQSMaxMessages       int
	SQSVisibilityTimeout time.Duration
	SQSProcessingTimeout time.Duration
	SQSRetryBase         time.Duration
	SQSRetryMax          time.Duration
	SQSTrustedProviders  []string

	OutboxEnabled      bool
	OutboxPollInterval time.Duration
	OutboxBatchSize    int
	OutboxLease        time.Duration
	OutboxRetryBase    time.Duration
	OutboxRetryMax     time.Duration

	PendingEnabled      bool
	PendingPollInterval time.Duration
	PendingBatchSize    int
	PendingReferenceTTL time.Duration
	PendingMaxAttempts  int
	PendingBackoffBase  time.Duration
	PendingBackoffMax   time.Duration

	StartTimeout    time.Duration
	ShutdownTimeout time.Duration
}

// Load lê as variáveis de ambiente, aplica valores padrão e valida o resultado.
func Load() (Config, error) {
	l := &loader{}
	host, _ := os.Hostname()
	c := Config{
		Env:        l.str("APP_ENV", "local"),
		InstanceID: l.str("INSTANCE_ID", host),
		LogLevel:   l.str("LOG_LEVEL", "info"),

		HTTPEnabled:        l.boolean("HTTP_ENABLED", true),
		HTTPAddr:           l.str("HTTP_ADDR", ":8080"),
		HTTPRequestTimeout: l.duration("HTTP_REQUEST_TIMEOUT", 10*time.Second),

		DatabaseURL:              l.str("DATABASE_URL", ""),
		DatabaseMaxConns:         l.integer("DATABASE_MAX_CONNS", 10),
		DatabaseLockTimeout:      l.duration("DATABASE_LOCK_TIMEOUT", 5*time.Second),
		DatabaseStatementTimeout: l.duration("DATABASE_STATEMENT_TIMEOUT", 15*time.Second),

		OIDCIssuerURL:     strings.TrimRight(l.str("OIDC_ISSUER_URL", ""), "/"),
		OIDCJWKSURL:       l.str("OIDC_JWKS_URL", ""),
		OIDCAudience:      l.str("OIDC_AUDIENCE", "wallet-api"),
		OIDCProviderClaim: l.str("OIDC_PROVIDER_CLAIM", "provider_id"),
		ProviderRole:      l.str("OIDC_PROVIDER_ROLE", "wagering-provider"),
		OperatorRole:      l.str("OIDC_OPERATOR_ROLE", "wallet-operator"),

		AWSRegion:      l.str("AWS_REGION", "us-east-1"),
		AWSEndpointURL: l.str("AWS_ENDPOINT_URL", ""),

		SQSWagerQueueName:    l.str("SQS_WAGER_QUEUE_NAME", "wager-transactions.fifo"),
		SQSWagerDLQName:      l.str("SQS_WAGER_DLQ_NAME", "wager-transactions-dlq.fifo"),
		SQSEventsQueueName:   l.str("SQS_EVENTS_QUEUE_NAME", "wallet-events.fifo"),
		SQSConsumerEnabled:   l.boolean("SQS_CONSUMER_ENABLED", true),
		SQSConsumerName:      l.str("SQS_CONSUMER_NAME", "wager-transactions-consumer"),
		SQSPollers:           l.integer("SQS_POLLERS", 2),
		SQSWaitTime:          l.duration("SQS_WAIT_TIME", 5*time.Second),
		SQSMaxMessages:       l.integer("SQS_MAX_MESSAGES", 10),
		SQSVisibilityTimeout: l.duration("SQS_VISIBILITY_TIMEOUT", 30*time.Second),
		SQSProcessingTimeout: l.duration("SQS_PROCESSING_TIMEOUT", 15*time.Second),
		SQSRetryBase:         l.duration("SQS_RETRY_BASE", 2*time.Second),
		SQSRetryMax:          l.duration("SQS_RETRY_MAX", 60*time.Second),
		SQSTrustedProviders:  l.list("SQS_TRUSTED_PROVIDERS"),

		OutboxEnabled:      l.boolean("OUTBOX_PUBLISHER_ENABLED", true),
		OutboxPollInterval: l.duration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		OutboxBatchSize:    l.integer("OUTBOX_BATCH_SIZE", 50),
		OutboxLease:        l.duration("OUTBOX_LEASE", 30*time.Second),
		OutboxRetryBase:    l.duration("OUTBOX_RETRY_BASE", time.Second),
		OutboxRetryMax:     l.duration("OUTBOX_RETRY_MAX", 5*time.Minute),

		PendingEnabled:      l.boolean("PENDING_WORKER_ENABLED", true),
		PendingPollInterval: l.duration("PENDING_POLL_INTERVAL", 500*time.Millisecond),
		PendingBatchSize:    l.integer("PENDING_BATCH_SIZE", 50),
		PendingReferenceTTL: l.duration("PENDING_REFERENCE_TTL", 5*time.Minute),
		PendingMaxAttempts:  l.integer("PENDING_REFERENCE_MAX_ATTEMPTS", 20),
		PendingBackoffBase:  l.duration("PENDING_BACKOFF_BASE", time.Second),
		PendingBackoffMax:   l.duration("PENDING_BACKOFF_MAX", 30*time.Second),

		StartTimeout:    l.duration("START_TIMEOUT", 60*time.Second),
		ShutdownTimeout: l.duration("SHUTDOWN_TIMEOUT", 25*time.Second),
	}
	if c.OIDCJWKSURL == "" && c.OIDCIssuerURL != "" {
		c.OIDCJWKSURL = c.OIDCIssuerURL + "/protocol/openid-connect/certs"
	}
	if err := errors.Join(append(l.errs, c.Validate())...); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Validate verifica valores obrigatórios e limites.
func (c Config) Validate() error {
	var errs []error
	req := map[string]string{
		"DATABASE_URL": c.DatabaseURL, "OIDC_ISSUER_URL": c.OIDCIssuerURL, "OIDC_AUDIENCE": c.OIDCAudience,
		"INSTANCE_ID": c.InstanceID, "SQS_WAGER_QUEUE_NAME": c.SQSWagerQueueName,
		"SQS_WAGER_DLQ_NAME": c.SQSWagerDLQName, "SQS_EVENTS_QUEUE_NAME": c.SQSEventsQueueName,
	}
	for k, v := range req {
		if strings.TrimSpace(v) == "" {
			errs = append(errs, fmt.Errorf("%s é obrigatório", k))
		}
	}
	for k, v := range map[string]string{"OIDC_ISSUER_URL": c.OIDCIssuerURL, "OIDC_JWKS_URL": c.OIDCJWKSURL} {
		if v == "" {
			continue
		}
		if u, err := url.Parse(v); err != nil || u.Scheme == "" || u.Host == "" {
			errs = append(errs, fmt.Errorf("%s deve ser uma URL absoluta", k))
		}
	}
	for k, v := range map[string]int{
		"DATABASE_MAX_CONNS": c.DatabaseMaxConns, "SQS_POLLERS": c.SQSPollers, "OUTBOX_BATCH_SIZE": c.OutboxBatchSize,
		"PENDING_BATCH_SIZE": c.PendingBatchSize, "PENDING_REFERENCE_MAX_ATTEMPTS": c.PendingMaxAttempts,
	} {
		if v <= 0 {
			errs = append(errs, fmt.Errorf("%s deve ser positivo", k))
		}
	}
	if c.SQSMaxMessages < 1 || c.SQSMaxMessages > 10 {
		errs = append(errs, errors.New("SQS_MAX_MESSAGES deve estar entre 1 e 10"))
	}
	if c.SQSWaitTime < 0 || c.SQSWaitTime > 20*time.Second {
		errs = append(errs, errors.New("SQS_WAIT_TIME deve estar entre 0 e 20s"))
	}
	if c.SQSProcessingTimeout >= c.SQSVisibilityTimeout {
		errs = append(errs, errors.New("SQS_PROCESSING_TIMEOUT deve ser menor que SQS_VISIBILITY_TIMEOUT"))
	}
	if c.ShutdownTimeout <= c.SQSWaitTime {
		errs = append(errs, errors.New("SHUTDOWN_TIMEOUT deve ser maior que SQS_WAIT_TIME"))
	}
	if c.SQSConsumerEnabled && len(c.SQSTrustedProviders) == 0 {
		errs = append(errs, errors.New("SQS_TRUSTED_PROVIDERS é obrigatório quando o consumidor SQS está habilitado"))
	}
	return errors.Join(errs...)
}

type loader struct{ errs []error }

func (l *loader) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(v)
	}
	return def
}

func (l *loader) integer(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: inteiro inválido %q", key, v))
		return def
	}
	return n
}

func (l *loader) boolean(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: booleano inválido %q", key, v))
		return def
	}
	return b
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil || d < 0 {
		l.errs = append(l.errs, fmt.Errorf("%s: duração inválida %q", key, v))
		return def
	}
	return d
}

func (l *loader) list(key string) []string {
	var out []string
	for _, p := range strings.Split(os.Getenv(key), ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
