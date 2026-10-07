// Package wager modela as operações financeiras (WagerTransaction) e sua
// máquina de estados.
package wager

import (
	"errors"
	"fmt"
)

// Kind é o tipo da operação.
type Kind string

// Tipos de operação. OPENING é exclusivo da abertura interna de carteira.
const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

// ParseKind valida qualquer tipo conhecido (usado na reidratação).
func ParseKind(s string) (Kind, error) {
	switch Kind(s) {
	case KindOpening, KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return Kind(s), nil
	default:
		return "", fmt.Errorf("%w: tipo %q", ErrInvalidKind, s)
	}
}

// ParseExternalKind valida um tipo recebido por HTTP ou SQS; OPENING é rejeitado.
func ParseExternalKind(s string) (Kind, error) {
	k, err := ParseKind(s)
	if err != nil {
		return "", err
	}
	if k == KindOpening {
		return "", ErrOpeningNotAllowed
	}
	return k, nil
}

// IsReversal indica REFUND ou ROLLBACK.
func (k Kind) IsReversal() bool { return k == KindRefund || k == KindRollback }

// Status é o estado da transação.
type Status string

// Estados possíveis.
const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

// ParseStatus valida um estado persistido.
func ParseStatus(s string) (Status, error) {
	switch Status(s) {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return Status(s), nil
	default:
		return "", fmt.Errorf("%w: estado %q", ErrInvalidTransaction, s)
	}
}

// IsTerminal indica PROCESSED, REJECTED ou FAILED.
func (s Status) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

// Origin distingue a abertura interna das operações externas dos provedores.
type Origin string

// Origens possíveis.
const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)

// FailureCode é o código estável persistido em transações REJECTED ou FAILED.
type FailureCode string

// Códigos de resultado definitivo (persistidos; replays devolvem o mesmo código).
const (
	// Regras de saldo.
	FailureInsufficientFunds         FailureCode = "INSUFFICIENT_FUNDS"
	FailureReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	// Coerência com a carteira.
	FailureCurrencyMismatch     FailureCode = "CURRENCY_MISMATCH"
	FailurePlayerWalletMismatch FailureCode = "PLAYER_WALLET_MISMATCH"
	// Referências.
	FailureReferenceNotFound        FailureCode = "REFERENCE_NOT_FOUND"
	FailureReferenceStillPending    FailureCode = "REFERENCE_STILL_PENDING"
	FailureReferenceNotProcessed    FailureCode = "REFERENCE_NOT_PROCESSED"
	FailureReferenceMismatch        FailureCode = "REFERENCE_MISMATCH"
	FailureReferenceKindNotAllowed  FailureCode = "REFERENCE_KIND_NOT_ALLOWED"
	FailureReferenceAmountMismatch  FailureCode = "REFERENCE_AMOUNT_MISMATCH"
	FailureReferenceAlreadyReversed FailureCode = "REFERENCE_ALREADY_REVERSED"
	// Falha permanente de infraestrutura (estado FAILED).
	FailureInternalPermanent FailureCode = "INTERNAL_PERMANENT_FAILURE"
)

var knownFailureCodes = map[FailureCode]struct{}{
	FailureInsufficientFunds: {}, FailureReversalInsufficientFunds: {}, FailureCurrencyMismatch: {},
	FailurePlayerWalletMismatch: {}, FailureReferenceNotFound: {}, FailureReferenceStillPending: {},
	FailureReferenceNotProcessed: {}, FailureReferenceMismatch: {}, FailureReferenceKindNotAllowed: {},
	FailureReferenceAmountMismatch: {}, FailureReferenceAlreadyReversed: {}, FailureInternalPermanent: {},
}

// ParseFailureCode valida um código persistido.
func ParseFailureCode(s string) (FailureCode, error) {
	if _, ok := knownFailureCodes[FailureCode(s)]; !ok {
		return "", fmt.Errorf("%w: failureCode %q", ErrInvalidTransaction, s)
	}
	return FailureCode(s), nil
}

// InputError é um erro de entrada corrigível: a requisição é recusada sem
// persistência (HTTP 400 / DLQ no SQS) e pode ser reenviada corrigida.
type InputError struct {
	code string
	msg  string
}

func (e *InputError) Error() string { return e.msg }

// Code devolve o código estável do erro de entrada.
func (e *InputError) Code() string { return e.code }

func newInputError(code, msg string) *InputError { return &InputError{code: code, msg: msg} }

// Erros de entrada (corrigíveis), classificáveis por errors.Is/errors.As.
var (
	ErrInvalidRequest       = newInputError("INVALID_REQUEST", "requisição inválida")
	ErrInvalidMoney         = newInputError("INVALID_MONEY", "valor monetário inválido")
	ErrInvalidKind          = newInputError("INVALID_KIND", "tipo de operação inválido")
	ErrOpeningNotAllowed    = newInputError("OPENING_NOT_ALLOWED", "OPENING é reservado à abertura interna de carteira")
	ErrInvalidAmountForKind = newInputError("INVALID_AMOUNT_FOR_KIND", "valor incompatível com o tipo de operação")
	ErrReferenceRequired    = newInputError("REFERENCE_REQUIRED", "referenceExternalTransactionId é obrigatório para REFUND e ROLLBACK")
	ErrReferenceNotAllowed  = newInputError("REFERENCE_NOT_ALLOWED", "referência não permitida para este tipo de operação")
)

// Erros de invariantes internas.
var (
	ErrInvalidTransaction = errors.New("wager: transação inválida")
	ErrInvalidTransition  = errors.New("wager: transição de estado inválida")
)
