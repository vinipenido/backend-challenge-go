// Package postgres implementa as portas de persistência com pgx e SQL explícito.
package postgres

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/vinipenido/backend-challenge-go/internal/app"
)

// classify traduz erros do driver para as categorias da aplicação:
//   - conflitos de concorrência (repetíveis pelo caso de uso): 40001, 40P01,
//     55P03 e violações de unicidade em corridas;
//   - falhas transitórias: conexão, timeouts, banco em shutdown/recuperação;
//   - demais erros do PostgreSQL (constraints, triggers) são permanentes.
func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return app.Transient(err)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001":
			return app.ConcurrencyConflict("serialization_failure", err)
		case "40P01":
			return app.ConcurrencyConflict("deadlock", err)
		case "55P03":
			return app.ConcurrencyConflict("lock_timeout", err)
		case "23505":
			if pgErr.ConstraintName == "wallets_player_currency_uk" {
				return app.ErrWalletAlreadyExists
			}
			return app.ConcurrencyConflict("unique:"+pgErr.ConstraintName, err)
		case "57014": // statement_timeout
			return app.Transient(err)
		case "57P01", "57P02", "57P03", "53300":
			return app.Transient(err)
		}
		if strings.HasPrefix(pgErr.Code, "08") || strings.HasPrefix(pgErr.Code, "53") {
			return app.Transient(err)
		}
		return err
	}
	if pgconn.Timeout(err) || pgconn.SafeToRetry(err) {
		return app.Transient(err)
	}
	var netErr net.Error
	var connErr *pgconn.ConnectError
	if errors.As(err, &netErr) || errors.As(err, &connErr) || errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		strings.Contains(err.Error(), "conn closed") || strings.Contains(err.Error(), "closed pool") {
		return app.Transient(err)
	}
	return err
}
