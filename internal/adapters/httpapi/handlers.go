package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/vinipenido/backend-challenge-go/internal/app"
	"github.com/vinipenido/backend-challenge-go/internal/auth"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wager"
	"github.com/vinipenido/backend-challenge-go/internal/platform/health"
)

func (s *Server) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
}

// ready verifica PostgreSQL e SQS; durante o encerramento devolve 503 para que
// o balanceador pare de enviar tráfego.
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	res := health.Result{Ready: true, Checks: map[string]string{}}
	if s.draining.Load() {
		res.Ready = false
		res.Checks["shutdown"] = "draining"
	}
	for _, c := range s.p.Checkers {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		err := c.Check(ctx)
		cancel()
		if err != nil {
			res.Ready = false
			res.Checks[c.Name] = "DOWN"
			continue
		}
		res.Checks[c.Name] = "UP"
	}
	status := http.StatusOK
	if !res.Ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, res)
}

func (s *Server) openWallet(w http.ResponseWriter, r *http.Request) {
	var body openWalletRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	view, err := s.p.Wallets.Open(r.Context(), app.OpenWalletCommand{
		PlayerID: body.PlayerID, InitialBalance: body.InitialBalance, CorrelationID: correlationID(r.Context()),
	})
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	w.Header().Set("Location", "/wallets/"+view.ID.String())
	writeJSON(w, http.StatusCreated, toWalletResponse(view))
}

func (s *Server) getWallet(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathUUID(w, r, "walletId")
	if !ok {
		return
	}
	view, err := s.p.Wallets.Get(r.Context(), id)
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toWalletResponse(view))
}

func (s *Server) listLedger(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathUUID(w, r, "walletId")
	if !ok {
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "limit deve estar entre 1 e 200", false)
			return
		}
		limit = n
	}
	page, err := s.p.Wallets.ListLedger(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	out := ledgerPageResponse{WalletID: id.String(), Entries: make([]ledgerEntryResponse, 0, len(page.Entries))}
	for _, e := range page.Entries {
		out.Entries = append(out.Entries, toLedgerEntry(e))
	}
	if page.NextCursor != "" {
		out.NextCursor = &page.NextCursor
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) reconcile(w http.ResponseWriter, r *http.Request) {
	id, ok := parsePathUUID(w, r, "walletId")
	if !ok {
		return
	}
	rec, err := s.p.Wallets.Reconcile(r.Context(), id, correlationID(r.Context()))
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, reconciliationResponse{
		WalletID: rec.WalletID.String(), StoredBalance: rec.StoredBalance, CalculatedBalance: rec.CalculatedBalance,
		Difference: rec.Difference, Consistent: rec.Consistent, CheckedEntries: rec.CheckedEntries,
	})
}

// submitTransaction recebe uma operação do provedor. O providerId do corpo
// precisa ser exatamente o providerId da identidade autenticada.
func (s *Server) submitTransaction(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	var body app.WagerRequestInput
	if !decodeJSON(w, r, &body) {
		return
	}
	if body.ProviderID != p.ProviderID {
		s.forbidden(w, r, "PROVIDER_MISMATCH", "providerId do corpo difere da identidade autenticada")
		return
	}
	req, err := app.ParseWagerRequest(body, r.Header.Get("Idempotency-Key"))
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	res, err := s.p.Wagering.Process(r.Context(), app.ProcessCommand{
		Request: req, Channel: app.ChannelHTTP, CorrelationID: correlationID(r.Context()),
	})
	if err != nil {
		if errors.Is(err, app.ErrPermanentFailure) && res.Transaction.Status == wager.StatusFailed {
			replay := false
			writeJSON(w, http.StatusInternalServerError, toTransactionResponse(res.Transaction, &replay))
			return
		}
		s.writeAppError(w, r, err)
		return
	}
	replay := res.Replay
	writeJSON(w, statusForTransaction(res.Transaction.Status, replay), toTransactionResponse(res.Transaction, &replay))
}

// statusForTransaction mapeia o estado persistido para o código HTTP:
// PROCESSED 201 (200 em replay), PENDING/PENDING_REFERENCE 202, REJECTED 422,
// FAILED 500.
func statusForTransaction(st wager.Status, replay bool) int {
	switch st {
	case wager.StatusProcessed:
		if replay {
			return http.StatusOK
		}
		return http.StatusCreated
	case wager.StatusPending, wager.StatusPendingReference:
		return http.StatusAccepted
	case wager.StatusRejected:
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}

// getTransaction: o operador vê qualquer transação; um provedor vê apenas as
// próprias. Transações de outros provedores respondem 404 (sem revelar existência).
func (s *Server) getTransaction(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	roles := s.p.Auth.Roles
	if !roles.IsOperator(p) && !roles.IsProvider(p) {
		s.forbidden(w, r, "FORBIDDEN", "papel insuficiente")
		return
	}
	id, ok := parsePathUUID(w, r, "transactionId")
	if !ok {
		return
	}
	snap, err := s.p.Queries.Get(r.Context(), id)
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	if !roles.IsOperator(p) && snap.ProviderID != p.ProviderID {
		s.p.Metrics.AuthFailures.WithLabelValues("cross_provider").Inc()
		writeError(w, http.StatusNotFound, "NOT_FOUND", "recurso não encontrado", false)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionResponse(snap, nil))
}

func (s *Server) getByExternalID(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	roles := s.p.Auth.Roles
	providerID := r.PathValue("providerId")
	switch {
	case roles.IsOperator(p):
	case roles.IsProvider(p) && p.ProviderID == providerID:
	default:
		s.forbidden(w, r, "PROVIDER_MISMATCH", "provedores consultam apenas as próprias transações")
		return
	}
	snap, err := s.p.Queries.GetByExternalID(r.Context(), providerID, r.PathValue("externalTransactionId"))
	if err != nil {
		s.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionResponse(snap, nil))
}
