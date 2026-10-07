// Package worker oferece um executor de goroutines de longa duração com
// cancelamento, prazo de parada e término observável.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

// Runner controla um conjunto de goroutines de um componente.
type Runner struct {
	name    string
	log     *slog.Logger
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	done    chan struct{}
	running atomic.Bool
}

// NewRunner cria o executor.
func NewRunner(name string, log *slog.Logger) *Runner {
	return &Runner{name: name, log: log, done: make(chan struct{})}
}

// Start inicia n goroutines executando fn com um contexto cancelável.
func (r *Runner) Start(n int, fn func(ctx context.Context, id int)) {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.running.Store(true)
	for i := range n {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			fn(ctx, i)
		}()
	}
	go func() {
		r.wg.Wait()
		r.running.Store(false)
		close(r.done)
	}()
	r.log.Info("worker iniciado", slog.String("worker", r.name), slog.Int("goroutines", n))
}

// Stop cancela o contexto (nenhum trabalho novo é buscado) e aguarda o término
// do trabalho em andamento até o prazo de ctx.
func (r *Runner) Stop(ctx context.Context) error {
	if r.cancel == nil {
		return nil
	}
	r.cancel()
	select {
	case <-r.done:
		r.log.Info("worker encerrado", slog.String("worker", r.name))
		return nil
	case <-ctx.Done():
		return fmt.Errorf("worker %s não terminou dentro do prazo: %w", r.name, ctx.Err())
	}
}

// Done é fechado quando todas as goroutines terminam.
func (r *Runner) Done() <-chan struct{} { return r.done }

// Running indica se ainda há goroutines ativas.
func (r *Runner) Running() bool { return r.running.Load() }
