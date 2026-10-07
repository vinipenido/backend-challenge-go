// Package migrator cria o executor de migrations (golang-migrate) com os
// arquivos SQL embutidos no binário.
package migrator

import (
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // driver pgx5://
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/vinipenido/backend-challenge-go/migrations"
)

// New cria o migrador. Aceita URLs postgres:// e as converte para o driver pgx v5.
func New(url string) (*migrate.Migrate, error) {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return nil, err
	}
	for _, p := range []string{"postgres://", "postgresql://"} {
		if strings.HasPrefix(url, p) {
			url = "pgx5://" + strings.TrimPrefix(url, p)
		}
	}
	return migrate.NewWithSourceInstance("iofs", src, url)
}
