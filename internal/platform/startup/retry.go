// Package startup oferece utilitários para validar dependências no início.
package startup

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Retry executa check até o sucesso, com nova tentativa a cada segundo.
//
// Uma fração do prazo do contexto é reservada: se o início falhar, o Fx usa o
// mesmo contexto para reverter (OnStop) os componentes já iniciados e ignora o
// rollback quando o contexto já expirou. Assim, recursos abertos (como o pool
// do PostgreSQL) são sempre liberados.
func Retry(ctx context.Context, log *slog.Logger, dependency string, check func(context.Context) error) error {
	if deadline, ok := ctx.Deadline(); ok {
		margin := min(max(time.Until(deadline)/4, 500*time.Millisecond), 5*time.Second)
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline.Add(-margin))
		defer cancel()
	}
	for {
		err := check(ctx)
		if err == nil {
			log.Info("dependência disponível", slog.String("dependency", dependency))
			return nil
		}
		log.Warn("dependência indisponível; nova tentativa", slog.String("dependency", dependency), slog.String("error", err.Error()))
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s indisponível no início: %w", dependency, err)
		case <-time.After(time.Second):
		}
	}
}
