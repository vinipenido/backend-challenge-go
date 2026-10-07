//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/vinipenido/backend-challenge-go/internal/adapters/httpapi"
	sqsadapter "github.com/vinipenido/backend-challenge-go/internal/adapters/sqs"
	"github.com/vinipenido/backend-challenge-go/internal/bootstrap"
	"github.com/vinipenido/backend-challenge-go/internal/config"
	"github.com/vinipenido/backend-challenge-go/internal/workers"
)

func loadConfig(t *testing.T, q queues, override map[string]string) config.Config {
	t.Helper()
	e := baseEnv(q)
	e["HTTP_ADDR"] = "127.0.0.1:0"
	e["INSTANCE_ID"] = "fx-test"
	e["LOG_LEVEL"] = "error"
	for k, v := range override {
		e[k] = v
	}
	for k, v := range e {
		t.Setenv(k, v)
	}
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestFxCompositionStartAndStop(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	cfg := loadConfig(t, q, nil)

	if err := fx.ValidateApp(bootstrap.Options(cfg)); err != nil {
		t.Fatalf("grafo de dependências inválido: %v", err)
	}

	var (
		consumer  *sqsadapter.Consumer
		publisher *sqsadapter.Publisher
		pending   *workers.PendingWorker
		server    *httpapi.Server
		pool      *pgxpool.Pool
	)
	app := bootstrap.New(cfg, fx.Populate(&consumer, &publisher, &pending, &server, &pool))
	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		t.Fatal(err)
	}
	for name, running := range map[string]bool{
		"consumer": consumer.Runner().Running(), "publisher": publisher.Runner().Running(), "pending": pending.Runner().Running(),
	} {
		if !running {
			t.Fatalf("worker %s não iniciou", name)
		}
	}
	resp, err := http.Get("http://" + server.Addr() + "/health/ready")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("readiness após start: %v %v", err, resp)
	}
	resp.Body.Close()

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelStop()
	if err := app.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	// Término observável dos workers.
	for name, done := range map[string]<-chan struct{}{
		"consumer": consumer.Runner().Done(), "publisher": publisher.Runner().Done(), "pending": pending.Runner().Done(),
	} {
		select {
		case <-done:
		default:
			t.Fatalf("worker %s ainda em execução após Stop", name)
		}
	}
	// Novas entradas interrompidas e dependências fechadas depois dos componentes.
	if _, err := http.Get("http://" + server.Addr() + "/health/live"); err == nil {
		t.Fatal("servidor HTTP deveria estar fechado")
	}
	if err := pool.Ping(context.Background()); err == nil {
		t.Fatal("pool do PostgreSQL deveria estar fechado")
	}
}

func TestFxStartFailsOnUnavailableDependency(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	cfg := loadConfig(t, q, map[string]string{"SQS_WAGER_QUEUE_NAME": "fila-inexistente.fifo", "START_TIMEOUT": "3s"})
	var pool *pgxpool.Pool
	app := bootstrap.New(cfg, fx.Populate(&pool))
	ctx, cancel := context.WithTimeout(context.Background(), cfg.StartTimeout)
	defer cancel()
	if err := app.Start(ctx); err == nil {
		t.Fatal("o início deveria falhar sem a fila provisionada")
	}
	// Os hooks já iniciados são revertidos: o pool aberto é fechado.
	if err := pool.Ping(context.Background()); err == nil {
		t.Fatal("pool deveria ter sido fechado no rollback do início")
	}
}

func TestConfigValidation(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	loadConfig(t, q, nil)
	t.Setenv("DATABASE_URL", "")
	t.Setenv("SQS_MAX_MESSAGES", "50")
	t.Setenv("SQS_WAIT_TIME", "abc")
	if _, err := config.Load(); err == nil {
		t.Fatal("configuração inválida deveria ser rejeitada")
	}
}
