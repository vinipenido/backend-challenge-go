// Package event define os eventos de integração produzidos pelo domínio.
//
// Cada evento possui um construtor próprio que fixa tipo e versão; o payload é
// tipado e, depois de serializado na outbox, torna-se um snapshot imutável.
package event

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/vinipenido/backend-challenge-go/internal/domain/money"
)

// Type identifica o tipo do evento.
type Type string

// Tipos de eventos exigidos.
const (
	TypeWagerTransactionProcessed        Type = "WagerTransactionProcessed"
	TypeWagerTransactionRejected         Type = "WagerTransactionRejected"
	TypeWalletBalanceChanged             Type = "WalletBalanceChanged"
	TypeWagerTransactionPendingReference Type = "WagerTransactionPendingReference"
)

// Tipos de agregado.
const (
	AggregateWagerTransaction = "WagerTransaction"
	AggregateWallet           = "Wallet"
)

// TimeFormat é RFC 3339 em UTC com milissegundos.
const TimeFormat = "2006-01-02T15:04:05.000Z07:00"

// FormatTime normaliza um instante para UTC/RFC 3339.
func FormatTime(t time.Time) string { return t.UTC().Format(TimeFormat) }

// Metadata são os metadados de rastreabilidade comuns a todos os eventos.
type Metadata struct {
	CorrelationID string
	CausationID   string // opcional
	OccurredAt    time.Time
}

// Envelope é o contrato publicado. Data contém uma das structs *Data tipadas.
type Envelope struct {
	EventID       uuid.UUID
	EventType     Type
	Version       int
	AggregateType string
	AggregateID   uuid.UUID
	// PartitionKey define a ordenação no destino (MessageGroupId): sempre o walletId,
	// para que todos os eventos de uma carteira sejam entregues em ordem.
	PartitionKey  string
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
	Data          any
}

type envelopeJSON struct {
	EventID       string  `json:"eventId"`
	EventType     Type    `json:"eventType"`
	AggregateType string  `json:"aggregateType"`
	AggregateID   string  `json:"aggregateId"`
	CorrelationID string  `json:"correlationId"`
	CausationID   *string `json:"causationId,omitempty"`
	OccurredAt    string  `json:"occurredAt"`
	Version       int     `json:"version"`
	Data          any     `json:"data"`
}

// MarshalJSON produz o envelope com timestamps UTC em RFC 3339.
func (e Envelope) MarshalJSON() ([]byte, error) {
	out := envelopeJSON{
		EventID:       e.EventID.String(),
		EventType:     e.EventType,
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID.String(),
		CorrelationID: e.CorrelationID,
		OccurredAt:    FormatTime(e.OccurredAt),
		Version:       e.Version,
		Data:          e.Data,
	}
	if e.CausationID != "" {
		c := e.CausationID
		out.CausationID = &c
	}
	return json.Marshal(out)
}

func newEnvelope(t Type, version int, aggType string, aggID uuid.UUID, walletID uuid.UUID, meta Metadata, data any) Envelope {
	return Envelope{
		EventID:       uuid.Must(uuid.NewV7()),
		EventType:     t,
		Version:       version,
		AggregateType: aggType,
		AggregateID:   aggID,
		PartitionKey:  walletID.String(),
		CorrelationID: meta.CorrelationID,
		CausationID:   meta.CausationID,
		OccurredAt:    meta.OccurredAt.UTC(),
		Data:          data,
	}
}

// WagerTransactionProcessedData é o payload de WagerTransactionProcessed (v1).
type WagerTransactionProcessedData struct {
	TransactionID                  string      `json:"transactionId"`
	Origin                         string      `json:"origin"`
	Kind                           string      `json:"kind"`
	WalletID                       string      `json:"walletId"`
	PlayerID                       string      `json:"playerId"`
	ProviderID                     string      `json:"providerId,omitempty"`
	ExternalTransactionID          string      `json:"externalTransactionId,omitempty"`
	RoundID                        string      `json:"roundId,omitempty"`
	GameID                         string      `json:"gameId,omitempty"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         string      `json:"referenceTransactionId,omitempty"`
	Money                          money.Money `json:"money"`
	BalanceAfter                   money.Money `json:"balanceAfter"`
	ProcessedAt                    string      `json:"processedAt"`
}

// NewWagerTransactionProcessed cria o evento com tipo e versão fixos.
func NewWagerTransactionProcessed(txID, walletID uuid.UUID, meta Metadata, data WagerTransactionProcessedData) Envelope {
	return newEnvelope(TypeWagerTransactionProcessed, 1, AggregateWagerTransaction, txID, walletID, meta, data)
}

// WagerTransactionRejectedData é o payload de WagerTransactionRejected (v1).
type WagerTransactionRejectedData struct {
	TransactionID                  string      `json:"transactionId"`
	Kind                           string      `json:"kind"`
	WalletID                       string      `json:"walletId"`
	PlayerID                       string      `json:"playerId"`
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
	Money                          money.Money `json:"money"`
	FailureCode                    string      `json:"failureCode"`
	RejectedAt                     string      `json:"rejectedAt"`
}

// NewWagerTransactionRejected cria o evento com tipo e versão fixos.
func NewWagerTransactionRejected(txID, walletID uuid.UUID, meta Metadata, data WagerTransactionRejectedData) Envelope {
	return newEnvelope(TypeWagerTransactionRejected, 1, AggregateWagerTransaction, txID, walletID, meta, data)
}

// WalletBalanceChangedData é o payload de WalletBalanceChanged (v1).
type WalletBalanceChangedData struct {
	WalletID      string      `json:"walletId"`
	TransactionID string      `json:"transactionId"`
	Direction     string      `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	WalletVersion int64       `json:"walletVersion"`
}

// NewWalletBalanceChanged cria o evento com tipo e versão fixos.
func NewWalletBalanceChanged(walletID uuid.UUID, meta Metadata, data WalletBalanceChangedData) Envelope {
	return newEnvelope(TypeWalletBalanceChanged, 1, AggregateWallet, walletID, walletID, meta, data)
}

// WagerTransactionPendingReferenceData é o payload de WagerTransactionPendingReference (v1).
type WagerTransactionPendingReferenceData struct {
	TransactionID                  string      `json:"transactionId"`
	Kind                           string      `json:"kind"`
	WalletID                       string      `json:"walletId"`
	PlayerID                       string      `json:"playerId"`
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId"`
	Money                          money.Money `json:"money"`
	NextAttemptAt                  string      `json:"nextAttemptAt"`
	ExpiresAt                      string      `json:"expiresAt"`
}

// NewWagerTransactionPendingReference cria o evento com tipo e versão fixos.
func NewWagerTransactionPendingReference(txID, walletID uuid.UUID, meta Metadata, data WagerTransactionPendingReferenceData) Envelope {
	return newEnvelope(TypeWagerTransactionPendingReference, 1, AggregateWagerTransaction, txID, walletID, meta, data)
}
