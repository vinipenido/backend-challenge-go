package app

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"

	"github.com/google/uuid"

	"github.com/vinipenido/backend-challenge-go/internal/domain/money"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wager"
)

// MoneyInput é o formato externo de valores monetários. amount é string;
// números JSON são recusados já na decodificação.
type MoneyInput struct {
	Amount   *string `json:"amount"`
	Currency *string `json:"currency"`
}

// WagerRequestInput são os campos de negócio de uma operação, comuns ao corpo
// HTTP e ao campo data da mensagem SQS.
type WagerRequestInput struct {
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	PlayerID                       string      `json:"playerId"`
	WalletID                       string      `json:"walletId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	Kind                           string      `json:"kind"`
	Money                          *MoneyInput `json:"money"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
}

// WagerRequest é a operação validada e normalizada, com o hash de idempotência.
type WagerRequest struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       uuid.UUID
	WalletID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           wager.Kind
	Money                          money.Money
	ReferenceExternalTransactionID string
	PayloadHash                    string
}

// ParseUUID aceita apenas a forma canônica de 36 caracteres (maiúsculas ou
// minúsculas) e normaliza para minúsculas.
func ParseUUID(field, s string) (uuid.UUID, error) {
	if len(s) != 36 {
		return uuid.Nil, fmt.Errorf("%w: %s deve ser um UUID canônico", wager.ErrInvalidRequest, field)
	}
	id, err := uuid.Parse(s)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: %s deve ser um UUID canônico", wager.ErrInvalidRequest, field)
	}
	return id, nil
}

// ParseMoneyInput converte MoneyInput em Money não negativo.
func ParseMoneyInput(field string, in *MoneyInput) (money.Money, error) {
	if in == nil || in.Amount == nil || in.Currency == nil {
		return money.Money{}, fmt.Errorf("%w: %s.amount e %s.currency são obrigatórios", wager.ErrInvalidMoney, field, field)
	}
	m, err := money.ParseNonNegative(*in.Amount, *in.Currency)
	if err != nil {
		return money.Money{}, fmt.Errorf("%w: %s: %v", wager.ErrInvalidMoney, field, err)
	}
	return m, nil
}

func validIdentifier(s string) bool {
	if s == "" || strings.TrimSpace(s) != s {
		return false
	}
	for _, r := range s {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

// ValidateIdempotencyKey exige chave não vazia, imprimível, sem espaços nas
// bordas e com até 255 caracteres. A chave é usada exatamente como recebida.
func ValidateIdempotencyKey(key string) error {
	if !validIdentifier(key) || len(key) > 255 {
		return fmt.Errorf("%w: Idempotency-Key obrigatória (1..255 caracteres imprimíveis)", wager.ErrInvalidRequest)
	}
	return nil
}

// ParseWagerRequest valida a entrada externa e calcula o hash canônico.
func ParseWagerRequest(in WagerRequestInput, idempotencyKey string) (WagerRequest, error) {
	if err := ValidateIdempotencyKey(idempotencyKey); err != nil {
		return WagerRequest{}, err
	}
	for name, v := range map[string]string{
		"providerId": in.ProviderID, "externalTransactionId": in.ExternalTransactionID,
		"roundId": in.RoundID, "gameId": in.GameID,
	} {
		if !validIdentifier(v) || len(v) > 128 {
			return WagerRequest{}, fmt.Errorf("%w: %s obrigatório (1..128 caracteres imprimíveis)", wager.ErrInvalidRequest, name)
		}
	}
	if in.ReferenceExternalTransactionID != "" && !validIdentifier(in.ReferenceExternalTransactionID) {
		return WagerRequest{}, fmt.Errorf("%w: referenceExternalTransactionId inválido", wager.ErrInvalidRequest)
	}
	playerID, err := ParseUUID("playerId", in.PlayerID)
	if err != nil {
		return WagerRequest{}, err
	}
	walletID, err := ParseUUID("walletId", in.WalletID)
	if err != nil {
		return WagerRequest{}, err
	}
	kind, err := wager.ParseExternalKind(in.Kind)
	if err != nil {
		return WagerRequest{}, err
	}
	m, err := ParseMoneyInput("money", in.Money)
	if err != nil {
		return WagerRequest{}, err
	}
	if err := wager.ValidateExternalShape(kind, m, in.ReferenceExternalTransactionID, in.ExternalTransactionID); err != nil {
		return WagerRequest{}, err
	}
	req := WagerRequest{
		ProviderID:                     in.ProviderID,
		ExternalTransactionID:          in.ExternalTransactionID,
		IdempotencyKey:                 idempotencyKey,
		PlayerID:                       playerID,
		WalletID:                       walletID,
		RoundID:                        in.RoundID,
		GameID:                         in.GameID,
		Kind:                           kind,
		Money:                          m,
		ReferenceExternalTransactionID: in.ReferenceExternalTransactionID,
	}
	req.PayloadHash = CanonicalHash(req)
	return req, nil
}

// CanonicalHash calcula SHA-256 (hex) do JSON canônico dos campos de negócio.
//
// Algoritmo: objeto JSON com chaves em ordem lexicográfica, sem espaços, sem
// escape de HTML, contendo exatamente: externalTransactionId, gameId, kind,
// money{amount,currency}, playerId, providerId, referenceExternalTransactionId
// (apenas quando presente), roundId e walletId. UUIDs em minúsculas; amount na
// forma canônica de duas casas. A chave de idempotência e metadados de
// transporte (headers, messageId, occurredAt, type) ficam de fora, garantindo
// o mesmo hash para HTTP e SQS.
func CanonicalHash(r WagerRequest) string {
	doc := map[string]any{
		"externalTransactionId": r.ExternalTransactionID,
		"gameId":                r.GameID,
		"kind":                  string(r.Kind),
		"money": map[string]any{
			"amount":   r.Money.Amount(),
			"currency": r.Money.Currency().Code(),
		},
		"playerId":   r.PlayerID.String(),
		"providerId": r.ProviderID,
		"roundId":    r.RoundID,
		"walletId":   r.WalletID.String(),
	}
	if r.ReferenceExternalTransactionID != "" {
		doc["referenceExternalTransactionId"] = r.ReferenceExternalTransactionID
	}
	return hashCanonical(doc)
}

// hashCanonical serializa com encoding/json (que ordena chaves de mapas) e
// calcula o SHA-256.
func hashCanonical(v any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v) // mapas de strings não falham na serialização
	sum := sha256.Sum256(bytes.TrimRight(buf.Bytes(), "\n"))
	return hex.EncodeToString(sum[:])
}

// CanonicalJSONHash interpreta um documento JSON arbitrário e devolve o hash
// da sua forma canônica (chaves ordenadas). Usado para o hash da mensagem na inbox.
func CanonicalJSONHash(raw []byte) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // preserva números exatamente como texto, sem float64
	var v any
	if err := dec.Decode(&v); err != nil {
		return "", err
	}
	return hashCanonical(v), nil
}
