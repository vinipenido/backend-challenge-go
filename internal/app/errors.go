package app

import (
	"context"
	"errors"
	"fmt"
)

// Erros de aplicação, classificáveis por errors.Is.
var (
	ErrNotFound                  = errors.New("recurso não encontrado")
	ErrWalletNotFound            = errors.New("carteira não encontrada")
	ErrWalletAlreadyExists       = errors.New("já existe carteira para o jogador nesta moeda")
	ErrIdempotencyConflict       = errors.New("Idempotency-Key já utilizada com conteúdo diferente")
	ErrExternalTransactionExists = errors.New("operação (providerId, externalTransactionId) já registrada com outra Idempotency-Key")
	ErrInboxHashConflict         = errors.New("messageId já processado com conteúdo diferente")
	ErrUntrustedProvider         = errors.New("provedor não autorizado neste canal")
	ErrInvalidCursor             = errors.New("cursor inválido")
	// ErrPermanentFailure indica falha permanente registrada como FAILED.
	ErrPermanentFailure = errors.New("falha permanente de processamento")
)

// TransientError marca falhas temporárias de infraestrutura (banco ou fila
// indisponível, timeout, conexão perdida). O chamador pode tentar novamente.
type TransientError struct{ Err error }

func (e *TransientError) Error() string { return "falha transitória: " + e.Err.Error() }
func (e *TransientError) Unwrap() error { return e.Err }

// Transient envolve err como transitório.
func Transient(err error) error {
	if err == nil {
		return nil
	}
	return &TransientError{Err: err}
}

// IsTransient indica se err é transitório (inclui cancelamento e timeout).
func IsTransient(err error) bool {
	var te *TransientError
	return errors.As(err, &te) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// ConcurrencyConflictError indica disputa entre escritores (violação de
// unicidade em corrida, serialização, deadlock, lock timeout ou versão
// divergente). O caso de uso repete a transação inteira de forma limitada.
type ConcurrencyConflictError struct {
	Reason string
	Err    error
}

func (e *ConcurrencyConflictError) Error() string {
	return fmt.Sprintf("conflito de concorrência (%s): %v", e.Reason, e.Err)
}
func (e *ConcurrencyConflictError) Unwrap() error { return e.Err }

// ConcurrencyConflict cria um ConcurrencyConflictError.
func ConcurrencyConflict(reason string, err error) error {
	return &ConcurrencyConflictError{Reason: reason, Err: err}
}

// IsConcurrencyConflict indica conflito de concorrência e devolve o motivo.
func IsConcurrencyConflict(err error) (string, bool) {
	var ce *ConcurrencyConflictError
	if errors.As(err, &ce) {
		return ce.Reason, true
	}
	return "", false
}
