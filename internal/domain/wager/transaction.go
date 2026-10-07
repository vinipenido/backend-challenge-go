package wager

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/vinipenido/backend-challenge-go/internal/domain/event"
	"github.com/vinipenido/backend-challenge-go/internal/domain/money"
)

// Limites de tamanho dos identificadores externos.
const maxExternalFieldLen = 128

// Transaction é uma operação financeira (externa ou de abertura interna).
//
// Máquina de estados:
//
//	PENDING ──► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE
//	PENDING_REFERENCE ──► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE (reagendamento)
//	PROCESSED, REJECTED, FAILED: terminais (nenhuma transição permitida)
type Transaction struct {
	s      Snapshot
	events []event.Envelope
}

// Snapshot é a representação plana e completa do estado (persistência/leitura).
type Snapshot struct {
	ID                             uuid.UUID
	Origin                         Origin
	Kind                           Kind
	Status                         Status
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	Money                          money.Money
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	RoundID                        string
	GameID                         string
	ReferenceExternalTransactionID string
	ReferenceTransactionID         *uuid.UUID
	FailureCode                    FailureCode
	ResultBalance                  *money.Money
	CorrelationID                  string
	CausationID                    string
	Attempts                       int
	NextAttemptAt                  *time.Time
	ExpiresAt                      *time.Time
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
	CompletedAt                    *time.Time
}

// ExternalParams descreve uma operação recebida de um provedor.
type ExternalParams struct {
	ID                             uuid.UUID
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Money                          money.Money
	ReferenceExternalTransactionID string
	CorrelationID                  string
	CausationID                    string
	Now                            time.Time
}

// ValidateExternalShape aplica as regras de entrada que não dependem do banco:
// tipo externo, política de valores por tipo e obrigatoriedade de referência.
func ValidateExternalShape(kind Kind, m money.Money, reference string, externalID string) error {
	if kind == KindOpening {
		return ErrOpeningNotAllowed
	}
	if _, err := ParseKind(string(kind)); err != nil {
		return err
	}
	if err := m.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMoney, err)
	}
	if m.IsNegative() {
		return fmt.Errorf("%w: valor negativo", ErrInvalidMoney)
	}
	switch kind {
	case KindLoss:
		if !m.IsZero() {
			return fmt.Errorf("%w: LOSS exige money.amount igual a \"0.00\"", ErrInvalidAmountForKind)
		}
	default:
		if !m.IsPositive() {
			return fmt.Errorf("%w: %s exige valor maior que zero", ErrInvalidAmountForKind, kind)
		}
	}
	switch kind {
	case KindRefund, KindRollback:
		if reference == "" {
			return ErrReferenceRequired
		}
	case KindBet:
		if reference != "" {
			return fmt.Errorf("%w: BET não aceita referência", ErrReferenceNotAllowed)
		}
	}
	if reference != "" && reference == externalID {
		return fmt.Errorf("%w: uma operação não pode referenciar a si mesma", ErrReferenceNotAllowed)
	}
	if len(reference) > maxExternalFieldLen {
		return fmt.Errorf("%w: referenceExternalTransactionId excede %d caracteres", ErrInvalidRequest, maxExternalFieldLen)
	}
	return nil
}

// NewExternal cria uma operação externa no estado PENDING.
func NewExternal(p ExternalParams) (*Transaction, error) {
	if p.ID == uuid.Nil || p.WalletID == uuid.Nil || p.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: id, walletId e playerId são obrigatórios", ErrInvalidRequest)
	}
	for name, v := range map[string]string{
		"providerId": p.ProviderID, "externalTransactionId": p.ExternalTransactionID,
		"idempotencyKey": p.IdempotencyKey, "roundId": p.RoundID, "gameId": p.GameID,
	} {
		if v == "" {
			return nil, fmt.Errorf("%w: %s é obrigatório", ErrInvalidRequest, name)
		}
		if len(v) > maxExternalFieldLen && name != "idempotencyKey" {
			return nil, fmt.Errorf("%w: %s excede %d caracteres", ErrInvalidRequest, name, maxExternalFieldLen)
		}
	}
	if len(p.IdempotencyKey) > 255 {
		return nil, fmt.Errorf("%w: idempotencyKey excede 255 caracteres", ErrInvalidRequest)
	}
	if len(p.PayloadHash) != 64 {
		return nil, fmt.Errorf("%w: payloadHash deve ser SHA-256 hexadecimal", ErrInvalidTransaction)
	}
	if err := ValidateExternalShape(p.Kind, p.Money, p.ReferenceExternalTransactionID, p.ExternalTransactionID); err != nil {
		return nil, err
	}
	if p.Now.IsZero() {
		return nil, fmt.Errorf("%w: instante obrigatório", ErrInvalidTransaction)
	}
	now := p.Now.UTC()
	return &Transaction{s: Snapshot{
		ID:                             p.ID,
		Origin:                         OriginExternal,
		Kind:                           p.Kind,
		Status:                         StatusPending,
		WalletID:                       p.WalletID,
		PlayerID:                       p.PlayerID,
		Money:                          p.Money,
		ProviderID:                     p.ProviderID,
		ExternalTransactionID:          p.ExternalTransactionID,
		IdempotencyKey:                 p.IdempotencyKey,
		PayloadHash:                    p.PayloadHash,
		RoundID:                        p.RoundID,
		GameID:                         p.GameID,
		ReferenceExternalTransactionID: p.ReferenceExternalTransactionID,
		CorrelationID:                  p.CorrelationID,
		CausationID:                    p.CausationID,
		CreatedAt:                      now,
		UpdatedAt:                      now,
	}}, nil
}

// NewOpening cria a transação interna de abertura (PENDING). Provedor, IDs e
// chaves externas, rodada, jogo e referência não se aplicam a essa origem.
func NewOpening(id, walletID, playerID uuid.UUID, amount money.Money, correlationID string, now time.Time) (*Transaction, error) {
	if id == uuid.Nil || walletID == uuid.Nil || playerID == uuid.Nil {
		return nil, fmt.Errorf("%w: identidades obrigatórias", ErrInvalidTransaction)
	}
	if err := amount.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMoney, err)
	}
	if !amount.IsPositive() {
		return nil, fmt.Errorf("%w: OPENING exige valor positivo", ErrInvalidAmountForKind)
	}
	if now.IsZero() {
		return nil, fmt.Errorf("%w: instante obrigatório", ErrInvalidTransaction)
	}
	now = now.UTC()
	return &Transaction{s: Snapshot{
		ID:            id,
		Origin:        OriginInternal,
		Kind:          KindOpening,
		Status:        StatusPending,
		WalletID:      walletID,
		PlayerID:      playerID,
		Money:         amount,
		CorrelationID: correlationID,
		CreatedAt:     now,
		UpdatedAt:     now,
	}}, nil
}

// Rehydrate reconstrói a transação a partir do estado persistido, validando
// a coerência dos campos, sem reaplicar transições nem emitir eventos.
func Rehydrate(s Snapshot) (*Transaction, error) {
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: identidades obrigatórias", ErrInvalidTransaction)
	}
	if _, err := ParseKind(string(s.Kind)); err != nil {
		return nil, err
	}
	if _, err := ParseStatus(string(s.Status)); err != nil {
		return nil, err
	}
	if err := s.Money.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTransaction, err)
	}
	switch s.Origin {
	case OriginInternal:
		if s.Kind != KindOpening || s.ProviderID != "" || s.ExternalTransactionID != "" {
			return nil, fmt.Errorf("%w: origem interna inconsistente", ErrInvalidTransaction)
		}
	case OriginExternal:
		if s.Kind == KindOpening || s.ProviderID == "" || s.ExternalTransactionID == "" || s.IdempotencyKey == "" {
			return nil, fmt.Errorf("%w: origem externa inconsistente", ErrInvalidTransaction)
		}
	default:
		return nil, fmt.Errorf("%w: origem %q", ErrInvalidTransaction, s.Origin)
	}
	if (s.Status == StatusRejected || s.Status == StatusFailed) != (s.FailureCode != "") {
		return nil, fmt.Errorf("%w: failureCode incoerente com o estado %s", ErrInvalidTransaction, s.Status)
	}
	if s.Status == StatusProcessed && s.ResultBalance == nil {
		return nil, fmt.Errorf("%w: transação processada sem saldo resultante", ErrInvalidTransaction)
	}
	return &Transaction{s: s}, nil
}

// --- Transições ---

func (t *Transaction) ensureTransition(to Status) error {
	from := t.s.Status
	if from.IsTerminal() {
		return fmt.Errorf("%w: %s é terminal (destino %s)", ErrInvalidTransition, from, to)
	}
	if t.s.Origin == OriginInternal && to != StatusProcessed && to != StatusFailed {
		return fmt.Errorf("%w: abertura interna não admite %s", ErrInvalidTransition, to)
	}
	return nil
}

func (t *Transaction) meta(now time.Time) event.Metadata {
	return event.Metadata{CorrelationID: t.s.CorrelationID, CausationID: t.s.CausationID, OccurredAt: now.UTC()}
}

// MarkProcessed conclui a operação com sucesso, registrando o saldo observado
// (devolvido em replays) e emitindo WagerTransactionProcessed.
func (t *Transaction) MarkProcessed(balanceAfter money.Money, referenceTxID *uuid.UUID, now time.Time) error {
	if err := t.ensureTransition(StatusProcessed); err != nil {
		return err
	}
	if err := balanceAfter.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTransaction, err)
	}
	if balanceAfter.Currency() != t.s.Money.Currency() || balanceAfter.IsNegative() {
		return fmt.Errorf("%w: saldo resultante incoerente", ErrInvalidTransaction)
	}
	now = now.UTC()
	t.s.Status = StatusProcessed
	t.s.ResultBalance = &balanceAfter
	t.s.ReferenceTransactionID = referenceTxID
	t.s.NextAttemptAt = nil
	t.s.UpdatedAt = now
	t.s.CompletedAt = &now
	data := event.WagerTransactionProcessedData{
		TransactionID:                  t.s.ID.String(),
		Origin:                         string(t.s.Origin),
		Kind:                           string(t.s.Kind),
		WalletID:                       t.s.WalletID.String(),
		PlayerID:                       t.s.PlayerID.String(),
		ProviderID:                     t.s.ProviderID,
		ExternalTransactionID:          t.s.ExternalTransactionID,
		RoundID:                        t.s.RoundID,
		GameID:                         t.s.GameID,
		ReferenceExternalTransactionID: t.s.ReferenceExternalTransactionID,
		Money:                          t.s.Money,
		BalanceAfter:                   balanceAfter,
		ProcessedAt:                    event.FormatTime(now),
	}
	if referenceTxID != nil {
		data.ReferenceTransactionID = referenceTxID.String()
	}
	t.events = append(t.events, event.NewWagerTransactionProcessed(t.s.ID, t.s.WalletID, t.meta(now), data))
	return nil
}

// MarkRejected encerra a operação por regra de negócio, emitindo
// WagerTransactionRejected. observedBalance é o saldo no momento da decisão.
func (t *Transaction) MarkRejected(code FailureCode, observedBalance *money.Money, referenceTxID *uuid.UUID, now time.Time) error {
	if err := t.ensureTransition(StatusRejected); err != nil {
		return err
	}
	if _, err := ParseFailureCode(string(code)); err != nil || code == FailureInternalPermanent {
		return fmt.Errorf("%w: código de rejeição inválido %q", ErrInvalidTransaction, code)
	}
	now = now.UTC()
	t.s.Status = StatusRejected
	t.s.FailureCode = code
	t.s.ResultBalance = observedBalance
	t.s.ReferenceTransactionID = referenceTxID
	t.s.NextAttemptAt = nil
	t.s.UpdatedAt = now
	t.s.CompletedAt = &now
	t.events = append(t.events, event.NewWagerTransactionRejected(t.s.ID, t.s.WalletID, t.meta(now), event.WagerTransactionRejectedData{
		TransactionID:                  t.s.ID.String(),
		Kind:                           string(t.s.Kind),
		WalletID:                       t.s.WalletID.String(),
		PlayerID:                       t.s.PlayerID.String(),
		ProviderID:                     t.s.ProviderID,
		ExternalTransactionID:          t.s.ExternalTransactionID,
		RoundID:                        t.s.RoundID,
		GameID:                         t.s.GameID,
		ReferenceExternalTransactionID: t.s.ReferenceExternalTransactionID,
		Money:                          t.s.Money,
		FailureCode:                    string(code),
		RejectedAt:                     event.FormatTime(now),
	}))
	return nil
}

// MarkFailed registra uma falha permanente de infraestrutura para auditoria.
func (t *Transaction) MarkFailed(code FailureCode, now time.Time) error {
	if err := t.ensureTransition(StatusFailed); err != nil {
		return err
	}
	if code != FailureInternalPermanent {
		return fmt.Errorf("%w: código de falha inválido %q", ErrInvalidTransaction, code)
	}
	now = now.UTC()
	t.s.Status = StatusFailed
	t.s.FailureCode = code
	t.s.NextAttemptAt = nil
	t.s.UpdatedAt = now
	t.s.CompletedAt = &now
	return nil
}

// MarkPendingReference coloca (ou mantém) a operação à espera da referência.
// A primeira entrada no estado emite WagerTransactionPendingReference; os
// reagendamentos seguintes apenas incrementam tentativas e o próximo horário.
func (t *Transaction) MarkPendingReference(nextAttemptAt, expiresAt, now time.Time) error {
	if err := t.ensureTransition(StatusPendingReference); err != nil {
		return err
	}
	if !t.s.Kind.IsReversal() && t.s.Kind != KindWin && t.s.Kind != KindLoss {
		return fmt.Errorf("%w: %s não depende de referência", ErrInvalidTransition, t.s.Kind)
	}
	if t.s.ReferenceExternalTransactionID == "" {
		return fmt.Errorf("%w: operação sem referência", ErrInvalidTransition)
	}
	now = now.UTC()
	first := t.s.Status == StatusPending
	next := nextAttemptAt.UTC()
	t.s.Status = StatusPendingReference
	t.s.NextAttemptAt = &next
	t.s.UpdatedAt = now
	if first {
		exp := expiresAt.UTC()
		t.s.ExpiresAt = &exp
		t.events = append(t.events, event.NewWagerTransactionPendingReference(t.s.ID, t.s.WalletID, t.meta(now), event.WagerTransactionPendingReferenceData{
			TransactionID:                  t.s.ID.String(),
			Kind:                           string(t.s.Kind),
			WalletID:                       t.s.WalletID.String(),
			PlayerID:                       t.s.PlayerID.String(),
			ProviderID:                     t.s.ProviderID,
			ExternalTransactionID:          t.s.ExternalTransactionID,
			ReferenceExternalTransactionID: t.s.ReferenceExternalTransactionID,
			Money:                          t.s.Money,
			NextAttemptAt:                  event.FormatTime(next),
			ExpiresAt:                      event.FormatTime(exp),
		}))
	} else {
		t.s.Attempts++
	}
	return nil
}

// PullEvents devolve e remove os eventos pendentes.
func (t *Transaction) PullEvents() []event.Envelope {
	out := t.events
	t.events = nil
	return out
}

// Snapshot devolve uma cópia do estado.
func (t *Transaction) Snapshot() Snapshot { return t.s }

// Métodos de acesso mais usados.

func (t *Transaction) ID() uuid.UUID       { return t.s.ID }
func (t *Transaction) Kind() Kind          { return t.s.Kind }
func (t *Transaction) Status() Status      { return t.s.Status }
func (t *Transaction) Money() money.Money  { return t.s.Money }
func (t *Transaction) WalletID() uuid.UUID { return t.s.WalletID }
func (t *Transaction) PlayerID() uuid.UUID { return t.s.PlayerID }
func (t *Transaction) ProviderID() string  { return t.s.ProviderID }
func (t *Transaction) RoundID() string     { return t.s.RoundID }
func (t *Transaction) PayloadHash() string { return t.s.PayloadHash }
func (t *Transaction) ReferenceExternalID() string {
	return t.s.ReferenceExternalTransactionID
}
func (t *Transaction) ExpiresAt() *time.Time { return t.s.ExpiresAt }
func (t *Transaction) Attempts() int         { return t.s.Attempts }
