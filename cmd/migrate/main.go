// Comando migrate: aplica e reverte as migrations versionadas embutidas no binário.
//
// Uso:
//
//	migrate up            aplica todas as migrations pendentes
//	migrate down [N]      reverte N migrations (padrão: 1)
//	migrate down-all      reverte todas (apenas ambientes locais/teste)
//	migrate goto V        migra até a versão V
//	migrate version       mostra a versão atual
//	migrate force V       marca a versão V como aplicada (recuperação manual)
//
// A conexão é lida de MIGRATIONS_DATABASE_URL (papel dono do schema).
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/golang-migrate/migrate/v4"

	"github.com/vinipenido/backend-challenge-go/internal/migrator"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "erro:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("comando obrigatório: up | down [N] | down-all | goto V | version | force V")
	}
	url := os.Getenv("MIGRATIONS_DATABASE_URL")
	if url == "" {
		return errors.New("MIGRATIONS_DATABASE_URL é obrigatório")
	}
	m, err := migrator.New(url)
	if err != nil {
		return err
	}
	defer m.Close()

	argInt := func(def int) (int, error) {
		if len(args) < 2 {
			return def, nil
		}
		return strconv.Atoi(args[1])
	}
	switch args[0] {
	case "up":
		err = m.Up()
	case "down":
		var n int
		if n, err = argInt(1); err == nil {
			err = m.Steps(-n)
		}
	case "down-all":
		err = m.Down()
	case "goto":
		var v int
		if v, err = argInt(-1); err == nil && v >= 0 {
			err = m.Migrate(uint(v))
		}
	case "force":
		var v int
		if v, err = argInt(-1); err == nil {
			err = m.Force(v)
		}
	case "version":
	default:
		return fmt.Errorf("comando desconhecido %q", args[0])
	}
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	v, dirty, verr := m.Version()
	if errors.Is(verr, migrate.ErrNilVersion) {
		fmt.Println("versão: nenhuma migration aplicada")
		return nil
	}
	if verr != nil {
		return verr
	}
	fmt.Printf("versão: %d (dirty=%v)\n", v, dirty)
	return nil
}
