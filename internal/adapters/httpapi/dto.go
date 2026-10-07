package httpapi

import (
	"github.com/vinipenido/backend-challenge-go/internal/app"
	"github.com/vinipenido/backend-challenge-go/internal/domain/event"
	"github.com/vinipenido/backend-challenge-go/internal/domain/money"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wager"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wallet"
)

// errorBody é o envelope de erro de todas as respostas sem recurso.
type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// openWalletRequest é o corpo de POST /wallets.
type openWalletRequest struct {
	PlayerID       string          `json:"playerId"`
	InitialBalance *app.MoneyInput `json:"initialBalance"`
}

type walletResponse struct {
	ID        string      `json:"id"`
	PlayerID  string      `json:"playerId"`
	Balance   money.Money `json:"balance"`
	Version   int64       `json:"version"`
	CreatedAt string      `json:"createdAt"`
	UpdatedAt string      `json:"updatedAt"`
}

func toWalletResponse(v app.WalletView) walletResponse {
	return walletResponse{
		ID: v.ID.String(), PlayerID: v.PlayerID.String(), Balance: v.Balance, Version: v.Version,
		CreatedAt: v.CreatedAt, UpdatedAt: v.UpdatedAt,
	}
}

type ledgerEntryResponse struct {
	ID            string      `json:"id"`
	WalletID      string      `json:"walletId"`
	TransactionID string      `json:"transactionId"`
	Direction     string      `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	WalletVersion int64       `json:"walletVersion"`
	CreatedAt     string      `json:"createdAt"`
}

type ledgerPageResponse struct {
	WalletID   string                `json:"walletId"`
	Entries    []ledgerEntryResponse `json:"entries"`
	NextCursor *string               `json:"nextCursor"`
}

func toLedgerEntry(e wallet.LedgerEntry) ledgerEntryResponse {
	return ledgerEntryResponse{
		ID: e.ID().String(), WalletID: e.WalletID().String(), TransactionID: e.TransactionID().String(),
		Direction: string(e.Direction()), Money: e.Amount(), BalanceBefore: e.BalanceBefore(),
		BalanceAfter: e.BalanceAfter(), WalletVersion: e.WalletVersion(), CreatedAt: event.FormatTime(e.CreatedAt()),
	}
}

type reconciliationResponse struct {
	WalletID          string      `json:"walletId"`
	StoredBalance     money.Money `json:"storedBalance"`
	CalculatedBalance money.Money `json:"calculatedBalance"`
	Difference        money.Money `json:"difference"`
	Consistent        bool        `json:"consistent"`
	CheckedEntries    int64       `json:"checkedEntries"`
}

// transactionResponse é a visão de uma transação; Balance é o saldo observado
// no processamento original (ou nulo enquanto pendente).
type transactionResponse struct {
	TransactionID                  string       `json:"transactionId"`
	Status                         string       `json:"status"`
	Balance                        *money.Money `json:"balance"`
	IdempotentReplay               *bool        `json:"idempotentReplay,omitempty"`
	FailureCode                    string       `json:"failureCode,omitempty"`
	Origin                         string       `json:"origin"`
	Kind                           string       `json:"kind"`
	ProviderID                     string       `json:"providerId,omitempty"`
	ExternalTransactionID          string       `json:"externalTransactionId,omitempty"`
	WalletID                       string       `json:"walletId"`
	PlayerID                       string       `json:"playerId"`
	RoundID                        string       `json:"roundId,omitempty"`
	GameID                         string       `json:"gameId,omitempty"`
	Money                          money.Money  `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         string       `json:"referenceTransactionId,omitempty"`
	Attempts                       int          `json:"attempts"`
	NextAttemptAt                  string       `json:"nextAttemptAt,omitempty"`
	ExpiresAt                      string       `json:"expiresAt,omitempty"`
	CreatedAt                      string       `json:"createdAt"`
	UpdatedAt                      string       `json:"updatedAt"`
	CompletedAt                    string       `json:"completedAt,omitempty"`
}

func toTransactionResponse(s wager.Snapshot, replay *bool) transactionResponse {
	r := transactionResponse{
		TransactionID: s.ID.String(), Status: string(s.Status), Balance: s.ResultBalance, IdempotentReplay: replay,
		FailureCode: string(s.FailureCode), Origin: string(s.Origin), Kind: string(s.Kind),
		ProviderID: s.ProviderID, ExternalTransactionID: s.ExternalTransactionID, WalletID: s.WalletID.String(),
		PlayerID: s.PlayerID.String(), RoundID: s.RoundID, GameID: s.GameID, Money: s.Money,
		ReferenceExternalTransactionID: s.ReferenceExternalTransactionID, Attempts: s.Attempts,
		CreatedAt: event.FormatTime(s.CreatedAt), UpdatedAt: event.FormatTime(s.UpdatedAt),
	}
	if s.ReferenceTransactionID != nil {
		r.ReferenceTransactionID = s.ReferenceTransactionID.String()
	}
	if s.NextAttemptAt != nil && s.Status == wager.StatusPendingReference {
		r.NextAttemptAt = event.FormatTime(*s.NextAttemptAt)
	}
	if s.ExpiresAt != nil {
		r.ExpiresAt = event.FormatTime(*s.ExpiresAt)
	}
	if s.CompletedAt != nil {
		r.CompletedAt = event.FormatTime(*s.CompletedAt)
	}
	return r
}
