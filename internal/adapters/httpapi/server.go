// Package httpapi expõe a API HTTP (net/http) com autenticação OIDC.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/google/uuid"
	"go.uber.org/fx"

	"github.com/vinipenido/backend-challenge-go/internal/app"
	"github.com/vinipenido/backend-challenge-go/internal/auth"
	"github.com/vinipenido/backend-challenge-go/internal/config"
	"github.com/vinipenido/backend-challenge-go/internal/domain/wager"
	"github.com/vinipenido/backend-challenge-go/internal/platform/health"
	"github.com/vinipenido/backend-challenge-go/internal/platform/metrics"
)

const maxBodyBytes = 64 << 10

// Params agrupa as dependências do servidor HTTP (injeção por construtor).
type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Config    config.Config
	Log       *slog.Logger
	Metrics   *metrics.Metrics
	Auth      *auth.Authenticator
	Wallets   *app.WalletService
	Wagering  *app.WageringService
	Queries   *app.TransactionQueries
	Checkers  []health.Checker `group:"readiness"`
}

// Server é o servidor HTTP da API.
type Server struct {
	p        Params
	srv      *http.Server
	draining atomic.Bool
	addr     atomic.Value
}

// NewServer cria o servidor e registra início/parada no ciclo de vida.
// OnStop interrompe novas conexões e aguarda as requisições em andamento
// (http.Server.Shutdown) dentro do prazo de encerramento.
func NewServer(p Params) *Server {
	s := &Server{p: p}
	s.srv = &http.Server{
		Handler:           s.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      p.Config.HTTPRequestTimeout + 5*time.Second,
		IdleTimeout:       60 * time.Second,
	}
	if !p.Config.HTTPEnabled {
		return s
	}
	p.Lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ln, err := net.Listen("tcp", p.Config.HTTPAddr)
			if err != nil {
				return fmt.Errorf("não foi possível escutar em %s: %w", p.Config.HTTPAddr, err)
			}
			s.addr.Store(ln.Addr().String())
			go func() {
				if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					p.Log.Error("servidor HTTP encerrado com erro", slog.String("error", err.Error()))
				}
			}()
			p.Log.Info("servidor HTTP iniciado", slog.String("addr", ln.Addr().String()))
			return nil
		},
		OnStop: func(ctx context.Context) error {
			s.draining.Store(true)
			err := s.srv.Shutdown(ctx)
			p.Log.Info("servidor HTTP encerrado", slog.Any("error", err))
			return err
		},
	})
	return s
}

// Addr devolve o endereço efetivo (útil quando HTTP_ADDR usa porta 0).
func (s *Server) Addr() string {
	v, _ := s.addr.Load().(string)
	return v
}

// Routes monta o roteador (padrões do net/http do Go 1.22+).
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	route := func(pattern string, h http.HandlerFunc) {
		mux.Handle(pattern, s.instrument(pattern, h))
	}
	// Públicos.
	route("GET /health/live", s.live)
	route("GET /health/ready", s.ready)
	mux.Handle("GET /metrics", s.p.Metrics.Handler())

	// Operações internas de carteira: apenas o serviço interno (papel wallet-operator).
	route("POST /wallets", s.authn(s.operatorOnly(s.openWallet)))
	route("GET /wallets/{walletId}", s.authn(s.operatorOnly(s.getWallet)))
	route("GET /wallets/{walletId}/ledger", s.authn(s.operatorOnly(s.listLedger)))
	route("POST /wallets/{walletId}/reconciliation", s.authn(s.operatorOnly(s.reconcile)))

	// Operações de provedores.
	route("POST /wagering/transactions", s.authn(s.providerOnly(s.submitTransaction)))
	route("GET /wagering/transactions/{transactionId}", s.authn(s.getTransaction))
	route("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", s.authn(s.getByExternalID))

	return s.withCorrelation(s.recoverer(mux))
}

// ---------------------------------------------------------------------------
// Interceptadores HTTP (middlewares)
// ---------------------------------------------------------------------------

type ctxKeyCorrelation struct{}

func correlationID(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyCorrelation{}).(string)
	return v
}

func validCorrelation(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for _, r := range s {
		if !unicode.IsPrint(r) || unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

// withCorrelation propaga X-Correlation-Id (ou gera um UUIDv7) e aplica o
// prazo máximo de cada requisição.
func (s *Server) withCorrelation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cid := r.Header.Get("X-Correlation-Id")
		if !validCorrelation(cid) {
			cid = uuid.Must(uuid.NewV7()).String()
		}
		w.Header().Set("X-Correlation-Id", cid)
		ctx, cancel := context.WithTimeout(r.Context(), s.p.Config.HTTPRequestTimeout)
		defer cancel()
		ctx = context.WithValue(ctx, ctxKeyCorrelation{}, cid)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.p.Log.Error("panic no handler HTTP", slog.Any("panic", rec), slog.String("correlationId", correlationID(r.Context())))
				writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "erro interno", false)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) instrument(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		d := time.Since(start)
		s.p.Metrics.HTTPRequests.WithLabelValues(route, strconv.Itoa(rec.status)).Inc()
		s.p.Metrics.HTTPDuration.WithLabelValues(route).Observe(d.Seconds())
		if route != "GET /health/live" && route != "GET /health/ready" {
			attrs := []any{
				slog.String("route", route), slog.Int("status", rec.status),
				slog.Int64("durationMs", d.Milliseconds()), slog.String("correlationId", correlationID(r.Context())),
			}
			if p, ok := auth.FromContext(r.Context()); ok && p.ProviderID != "" {
				attrs = append(attrs, slog.String("providerId", p.ProviderID))
			}
			s.p.Log.Info("requisição HTTP", attrs...)
		}
	})
}

// authn exige um access token válido do IdP. Falhas devolvem 401 sem tocar em
// nenhum dado.
func (s *Server) authn(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := s.p.Auth.Authenticate(r.Context(), auth.BearerToken(r))
		if err != nil {
			reason := "invalid_token"
			switch {
			case errors.Is(err, auth.ErrMissingToken):
				reason = "missing_token"
			case errors.Is(err, auth.ErrExpiredToken):
				reason = "expired_token"
			}
			s.p.Metrics.AuthFailures.WithLabelValues(reason).Inc()
			s.p.Log.Warn("autenticação recusada", slog.String("reason", reason), slog.String("correlationId", correlationID(r.Context())))
			w.Header().Set("WWW-Authenticate", `Bearer realm="wallet-api", error="invalid_token"`)
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "token de acesso ausente, inválido ou expirado", false)
			return
		}
		next(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
	}
}

func (s *Server) forbidden(w http.ResponseWriter, r *http.Request, code, msg string) {
	s.p.Metrics.AuthFailures.WithLabelValues("forbidden").Inc()
	p, _ := auth.FromContext(r.Context())
	s.p.Log.Warn("acesso negado", slog.String("code", code), slog.String("clientId", p.ClientID),
		slog.String("correlationId", correlationID(r.Context())))
	writeError(w, http.StatusForbidden, code, msg, false)
}

func (s *Server) operatorOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, _ := auth.FromContext(r.Context())
		if !s.p.Auth.Roles.IsOperator(p) {
			s.forbidden(w, r, "FORBIDDEN", "operação restrita ao serviço interno de carteiras")
			return
		}
		next(w, r)
	}
}

func (s *Server) providerOnly(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, _ := auth.FromContext(r.Context())
		if !s.p.Auth.Roles.IsProvider(p) {
			s.forbidden(w, r, "FORBIDDEN", "operação restrita a provedores de jogos")
			return
		}
		next(w, r)
	}
}

// ---------------------------------------------------------------------------
// Utilitários de resposta
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string, retryable bool) {
	if retryable {
		w.Header().Set("Retry-After", "1")
	}
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: msg, Retryable: retryable}})
}

// writeAppError traduz erros de aplicação para o contrato HTTP.
func (s *Server) writeAppError(w http.ResponseWriter, r *http.Request, err error) {
	var ie *wager.InputError
	switch {
	case errors.As(err, &ie):
		writeError(w, http.StatusBadRequest, ie.Code(), err.Error(), false)
	case errors.Is(err, app.ErrInvalidCursor):
		writeError(w, http.StatusBadRequest, "INVALID_CURSOR", err.Error(), false)
	case errors.Is(err, app.ErrWalletNotFound):
		writeError(w, http.StatusNotFound, "WALLET_NOT_FOUND", err.Error(), false)
	case errors.Is(err, app.ErrNotFound):
		writeError(w, http.StatusNotFound, "NOT_FOUND", "recurso não encontrado", false)
	case errors.Is(err, app.ErrWalletAlreadyExists):
		writeError(w, http.StatusConflict, "WALLET_ALREADY_EXISTS", err.Error(), false)
	case errors.Is(err, app.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "IDEMPOTENCY_KEY_CONFLICT", err.Error(), false)
	case errors.Is(err, app.ErrExternalTransactionExists):
		writeError(w, http.StatusConflict, "EXTERNAL_TRANSACTION_CONFLICT", err.Error(), false)
	case app.IsTransient(err):
		writeError(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "dependência temporariamente indisponível; tente novamente com a mesma Idempotency-Key", true)
	default:
		s.p.Log.Error("erro interno", slog.String("error", err.Error()), slog.String("correlationId", correlationID(r.Context())))
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "erro interno", false)
	}
}

// decodeJSON aplica limite de tamanho, Content-Type e rejeita campos desconhecidos.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE", "Content-Type deve ser application/json", false)
		return false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "corpo excede o limite", false)
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var te *json.UnmarshalTypeError
		if errors.As(err, &te) && (te.Field == "money.amount" || te.Field == "initialBalance.amount") {
			writeError(w, http.StatusBadRequest, "INVALID_MONEY", "amount deve ser uma string decimal (ex.: \"25.00\")", false)
			return false
		}
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "JSON inválido: "+err.Error(), false)
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "JSON inválido: conteúdo após o objeto", false)
		return false
	}
	return true
}

func parsePathUUID(w http.ResponseWriter, r *http.Request, name string) (uuid.UUID, bool) {
	id, err := app.ParseUUID(name, r.PathValue(name))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error(), false)
		return uuid.Nil, false
	}
	return id, true
}

// Module registra o servidor HTTP no Fx.
var Module = fx.Module("http",
	fx.Provide(NewServer),
	fx.Invoke(func(*Server) {}),
)
