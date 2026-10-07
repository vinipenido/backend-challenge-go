// Package migrations embute os arquivos SQL versionados no binário.
package migrations

import "embed"

// FS contém as migrations no formato {versão}_{nome}.{up|down}.sql.
//
//go:embed *.sql
var FS embed.FS
