package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/vinipenido/backend-challenge-go/internal/app"
	"github.com/vinipenido/backend-challenge-go/internal/config"
	"github.com/vinipenido/backend-challenge-go/internal/platform/startup"
)

// NewPool cria o pool de conexões e registra no ciclo de vida do Fx:
//   - OnStart: valida a conectividade (com novas tentativas até o prazo de início);
//   - OnStop: fecha o pool. Como o Fx executa os OnStop em ordem inversa, o pool
//     só é fechado depois que servidor HTTP e workers (que dependem dele) pararam.
func NewPool(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("DATABASE_URL inválida: %w", err)
	}
	pcfg.MaxConns = int32(cfg.DatabaseMaxConns)
	pcfg.MinConns = 1
	pcfg.MaxConnIdleTime = time.Minute
	pcfg.HealthCheckPeriod = 10 * time.Second
	pcfg.ConnConfig.ConnectTimeout = 3 * time.Second
	// Proteções por sessão: nenhuma espera indefinida por lock ou consulta.
	pcfg.ConnConfig.RuntimeParams["lock_timeout"] = fmt.Sprintf("%d", cfg.DatabaseLockTimeout.Milliseconds())
	pcfg.ConnConfig.RuntimeParams["statement_timeout"] = fmt.Sprintf("%d", cfg.DatabaseStatementTimeout.Milliseconds())
	pcfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "30000"
	pcfg.ConnConfig.RuntimeParams["application_name"] = cfg.InstanceID

	pool, err := pgxpool.NewWithConfig(context.Background(), pcfg)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return startup.Retry(ctx, log, "postgres", pool.Ping)
		},
		OnStop: func(context.Context) error {
			pool.Close()
			log.Info("pool do PostgreSQL fechado")
			return nil
		},
	})
	return pool, nil
}

// Module registra os componentes de persistência no Fx. O Invoke força a
// criação do pool antes dos demais componentes: seu hook inicia primeiro (a
// dependência é validada antes de qualquer worker) e para por último.
var Module = fx.Module("postgres",
	fx.Provide(
		NewPool,
		NewOutboxStore,
		fx.Annotate(NewUnitOfWork, fx.As(new(app.UnitOfWork))),
	),
	fx.Invoke(func(*pgxpool.Pool) {}),
)
