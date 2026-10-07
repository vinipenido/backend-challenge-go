//go:build faultinject

package fault

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
)

// ErrInjected é o erro devolvido por pontos de erro ativos.
var ErrInjected = errors.New("falha injetada")

func active(env, point string) bool {
	for _, p := range strings.Split(os.Getenv(env), ",") {
		if strings.TrimSpace(p) == point {
			return true
		}
	}
	return false
}

// Crash simula um encerramento abrupto (sem defers, sem shutdown gracioso),
// enviando SIGKILL ao próprio processo quando o ponto está em FAULT_CRASH_POINTS.
func Crash(point string) {
	if active("FAULT_CRASH_POINTS", point) {
		fmt.Fprintf(os.Stderr, "{\"msg\":\"fault: crash injetado\",\"point\":%q}\n", point)
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {} // aguarda o sinal
	}
}

// Error devolve ErrInjected quando o ponto está em FAULT_ERROR_POINTS.
func Error(point string) error {
	if active("FAULT_ERROR_POINTS", point) {
		return fmt.Errorf("%w: %s", ErrInjected, point)
	}
	return nil
}

// Enabled informa se a injeção de falhas foi compilada no binário.
func Enabled() bool { return true }
