// Package workers contém os workers de domínio executados em segundo plano.
package workers

import (
	"context"
	"log/slog"
	"time"

	"go.uber.org/fx"

	"github.com/vinipenido/backend-challenge-go/internal/app"
	"github.com/vinipenido/backend-challenge-go/internal/config"
	"github.com/vinipenido/backend-challenge-go/internal/platform/worker"
)

// PendingWorker retoma operações PENDING_REFERENCE (e qualquer PENDING
// confirmado) com backoff exponencial persistido em next_attempt_at. Como o
// estado está no banco, a retomada sobrevive a reinícios e pode ser feita por
// qualquer instância.
type PendingWorker struct {
	cfg      config.Config
	log      *slog.Logger
	wagering *app.WageringService
	runner   *worker.Runner
}

// NewPendingWorker cria o worker e registra seu ciclo de vida.
func NewPendingWorker(lc fx.Lifecycle, cfg config.Config, log *slog.Logger, wagering *app.WageringService) *PendingWorker {
	w := &PendingWorker{cfg: cfg, log: log.With(slog.String("component", "pending-reference-worker")), wagering: wagering}
	w.runner = worker.NewRunner("pending-reference-worker", w.log)
	if !cfg.PendingEnabled {
		return w
	}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			w.runner.Start(1, func(ctx context.Context, _ int) { w.loop(ctx) })
			return nil
		},
		OnStop: w.runner.Stop,
	})
	return w
}

// Runner expõe o executor (verificação de término nos testes).
func (w *PendingWorker) Runner() *worker.Runner { return w.runner }

func (w *PendingWorker) loop(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := w.wagering.ResolvePending(ctx, w.cfg.PendingBatchSize)
		if err != nil && ctx.Err() == nil {
			w.log.Warn("falha ao retomar pendências", slog.String("error", err.Error()))
			wait(ctx, 2*time.Second)
			continue
		}
		if n < w.cfg.PendingBatchSize {
			wait(ctx, w.cfg.PendingPollInterval)
		}
	}
}

func wait(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// Module registra os workers no Fx.
var Module = fx.Module("workers",
	fx.Provide(NewPendingWorker),
	fx.Invoke(func(*PendingWorker) {}),
)
