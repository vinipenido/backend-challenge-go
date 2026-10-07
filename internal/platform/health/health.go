// Package health define as verificações de prontidão (readiness) das dependências.
package health

import (
	"context"

	"go.uber.org/fx"
)

// Checker verifica uma dependência (PostgreSQL, SQS...).
type Checker struct {
	Name  string
	Check func(ctx context.Context) error
}

// Result é o resultado agregado das verificações.
type Result struct {
	Ready  bool              `json:"ready"`
	Checks map[string]string `json:"checks"`
}

// AsChecker anota um construtor para o grupo Fx "readiness".
func AsChecker(f any) any {
	return fx.Annotate(f, fx.ResultTags(`group:"readiness"`))
}
