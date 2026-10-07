package wager

import "github.com/google/uuid"

// Movement indica o efeito financeiro que a operação terá sobre a carteira.
type Movement int

// Efeitos possíveis.
const (
	MovementNone Movement = iota
	MovementDebit
	MovementCredit
)

// Outcome é o resultado da avaliação de regras de uma operação.
type Outcome int

// Resultados possíveis.
const (
	// OutcomeApply: a operação pode ser aplicada com o Movement indicado.
	OutcomeApply Outcome = iota
	// OutcomeWait: a referência ainda não está disponível (ou ainda está pendente).
	OutcomeWait
	// OutcomeReject: a operação deve ser rejeitada com FailureCode.
	OutcomeReject
)

// Decision descreve como aplicar uma operação.
type Decision struct {
	Outcome     Outcome
	Movement    Movement
	FailureCode FailureCode
	// ReferenceID é a referência interna resolvida, quando existir.
	ReferenceID *uuid.UUID
	// InsufficientFundsCode é o código usado se o débito exceder o saldo:
	// INSUFFICIENT_FUNDS para BET e REVERSAL_INSUFFICIENT_FUNDS para reversões.
	InsufficientFundsCode FailureCode
}

// ReferenceState é a visão mínima da transação referenciada necessária às regras.
type ReferenceState struct {
	Snapshot Snapshot
	// HasProcessedReversal indica se a referência já recebeu uma reversão
	// (REFUND ou ROLLBACK) concluída com sucesso.
	HasProcessedReversal bool
}

// Decide aplica as regras da seção "Operações e referências".
//
// ref == nil significa que a referência ainda não foi encontrada.
// Política de combinações REFUND/ROLLBACK (ver ARCHITECTURE.md):
//   - uma BET admite no máximo UMA reversão bem-sucedida, seja REFUND ou ROLLBACK;
//   - WIN e REFUND admitem no máximo um ROLLBACK;
//   - ROLLBACK, LOSS e OPENING não são reversíveis.
func (t *Transaction) Decide(ref *ReferenceState) Decision {
	s := t.s
	base := Decision{Outcome: OutcomeApply, InsufficientFundsCode: FailureInsufficientFunds}

	if s.ReferenceExternalTransactionID == "" {
		switch s.Kind {
		case KindBet:
			base.Movement = MovementDebit
		case KindWin:
			base.Movement = MovementCredit
		case KindLoss:
			base.Movement = MovementNone
		default:
			// REFUND/ROLLBACK sem referência já são barrados na entrada.
			return Decision{Outcome: OutcomeReject, FailureCode: FailureReferenceNotFound}
		}
		return base
	}

	if ref == nil {
		return Decision{Outcome: OutcomeWait}
	}
	r := ref.Snapshot
	switch r.Status {
	case StatusPending, StatusPendingReference:
		return Decision{Outcome: OutcomeWait}
	case StatusRejected, StatusFailed:
		return reject(FailureReferenceNotProcessed, r.ID)
	}
	if r.ProviderID != s.ProviderID || r.PlayerID != s.PlayerID || r.WalletID != s.WalletID ||
		r.Money.Currency() != s.Money.Currency() || r.RoundID != s.RoundID {
		return reject(FailureReferenceMismatch, r.ID)
	}

	refID := r.ID
	base.ReferenceID = &refID
	switch s.Kind {
	case KindWin, KindLoss:
		if r.Kind != KindBet {
			return reject(FailureReferenceKindNotAllowed, r.ID)
		}
		if s.Kind == KindWin {
			base.Movement = MovementCredit
		}
		return base
	case KindRefund:
		if r.Kind != KindBet {
			return reject(FailureReferenceKindNotAllowed, r.ID)
		}
		base.Movement = MovementCredit
	case KindRollback:
		switch r.Kind {
		case KindBet:
			base.Movement = MovementCredit
		case KindWin, KindRefund:
			base.Movement = MovementDebit
		default:
			return reject(FailureReferenceKindNotAllowed, r.ID)
		}
	default:
		return reject(FailureReferenceKindNotAllowed, r.ID)
	}
	// Regras comuns às reversões: valor integral e reversão única.
	if !r.Money.Equal(s.Money) {
		return reject(FailureReferenceAmountMismatch, r.ID)
	}
	if ref.HasProcessedReversal {
		return reject(FailureReferenceAlreadyReversed, r.ID)
	}
	base.InsufficientFundsCode = FailureReversalInsufficientFunds
	return base
}

func reject(code FailureCode, refID uuid.UUID) Decision {
	id := refID
	return Decision{Outcome: OutcomeReject, FailureCode: code, ReferenceID: &id}
}
