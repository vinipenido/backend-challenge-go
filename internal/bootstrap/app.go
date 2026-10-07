// Package bootstrap compõe a aplicação com Uber Fx.
package bootstrap

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/vinipenido/backend-challenge-go/internal/adapters/httpapi"
	"github.com/vinipenido/backend-challenge-go/internal/adapters/postgres"
	sqsadapter "github.com/vinipenido/backend-challenge-go/internal/adapters/sqs"
	"github.com/vinipenido/backend-challenge-go/internal/app"
	"github.com/vinipenido/backend-challenge-go/internal/auth"
	"github.com/vinipenido/backend-challenge-go/internal/config"
	"github.com/vinipenido/backend-challenge-go/internal/platform/health"
	"github.com/vinipenido/backend-challenge-go/internal/platform/logging"
	"github.com/vinipenido/backend-challenge-go/internal/platform/metrics"
	"github.com/vinipenido/backend-challenge-go/internal/workers"
)

// PlatformModule fornece configuração, logs, métricas, relógio e IDs.
var PlatformModule = fx.Module("platform",
	fx.Provide(
		logging.New,
		metrics.New,
		fx.Annotate(func() app.SystemClock { return app.SystemClock{} }, fx.As(new(app.Clock))),
		fx.Annotate(func() app.UUIDv7 { return app.UUIDv7{} }, fx.As(new(app.IDGenerator))),
		fx.Annotate(func(m *metrics.Metrics) *metrics.Metrics { return m }, fx.As(new(app.Metrics))),
		health.AsChecker(func(pool *pgxpool.Pool) health.Checker {
			return health.Checker{Name: "postgres", Check: func(ctx context.Context) error { return pool.Ping(ctx) }}
		}),
	),
)

// AppModule fornece os casos de uso.
var AppModule = fx.Module("app",
	fx.Provide(
		func(cfg config.Config) app.WageringConfig {
			return app.WageringConfig{
				MaxTxAttempts:               5,
				PendingReferenceTTL:         cfg.PendingReferenceTTL,
				PendingReferenceMaxAttempts: cfg.PendingMaxAttempts,
				PendingBackoffBase:          cfg.PendingBackoffBase,
				PendingBackoffMax:           cfg.PendingBackoffMax,
			}
		},
		app.NewWageringService,
		app.NewWalletService,
		app.NewTransactionQueries,
	),
)

// Options devolve todos os módulos da aplicação para a configuração dada.
// A ordem dos módulos não importa para o Fx; a ordem dos hooks segue as
// dependências: recursos (pool, filas, JWKS) iniciam antes e param depois de
// servidor e workers.
func Options(cfg config.Config) fx.Option {
	return fx.Options(
		fx.Supply(cfg),
		fx.StartTimeout(cfg.StartTimeout),
		fx.StopTimeout(cfg.ShutdownTimeout),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger {
			l := &fxevent.SlogLogger{Logger: log.With(slog.String("component", "fx"))}
			l.UseLogLevel(slog.LevelDebug)
			return l
		}),
		PlatformModule,
		postgres.Module,
		auth.Module,
		AppModule,
		sqsadapter.Module,
		workers.Module,
		httpapi.Module,
	)
}

// New cria a aplicação Fx.
func New(cfg config.Config, extra ...fx.Option) *fx.App {
	return fx.New(append([]fx.Option{Options(cfg)}, extra...)...)
}
