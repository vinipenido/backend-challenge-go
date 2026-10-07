// Package logging configura logs estruturados em JSON (log/slog).
package logging

import (
	"log/slog"
	"os"
	"strings"

	"github.com/vinipenido/backend-challenge-go/internal/config"
)

// New cria o logger JSON com os atributos fixos do serviço e da instância.
// Os demais identificadores (correlationId, messageId, transactionId, walletId,
// providerId) são adicionados por quem registra cada evento. Credenciais e
// payloads financeiros completos nunca são registrados.
func New(cfg config.Config) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(h).With(slog.String("service", "wallet-service"), slog.String("instanceId", cfg.InstanceID))
}
