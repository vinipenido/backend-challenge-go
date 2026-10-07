// Comando wallet-service: API HTTP, consumidor SQS, publicador da outbox e
// worker de referências pendentes, compostos com Uber Fx.
package main

import (
	"fmt"
	"os"

	"github.com/vinipenido/backend-challenge-go/internal/bootstrap"
	"github.com/vinipenido/backend-challenge-go/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "{\"level\":\"ERROR\",\"msg\":\"configuração inválida\",\"error\":%q}\n", err.Error())
		os.Exit(2)
	}
	// Run inicia os componentes, aguarda SIGINT/SIGTERM e executa o shutdown
	// ordenado dentro de SHUTDOWN_TIMEOUT.
	bootstrap.New(cfg).Run()
}
