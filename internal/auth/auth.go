// Package auth valida tokens de acesso emitidos pelo IdP externo (OIDC) e
// define o modelo de permissões da API.
package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"go.uber.org/fx"

	"github.com/vinipenido/backend-challenge-go/internal/config"
	"github.com/vinipenido/backend-challenge-go/internal/platform/startup"
)

// Erros de autenticação (HTTP 401).
var (
	ErrMissingToken = errors.New("token de acesso ausente")
	ErrInvalidToken = errors.New("token de acesso inválido")
	ErrExpiredToken = errors.New("token de acesso expirado")
)

// Principal é a identidade autenticada extraída do token.
type Principal struct {
	Subject  string
	ClientID string
	// ProviderID vem exclusivamente do claim configurado (provider_id), definido
	// no IdP por cliente; nunca do corpo da requisição.
	ProviderID string
	Roles      []string
}

// HasRole indica se o principal possui o papel.
func (p Principal) HasRole(role string) bool {
	for _, r := range p.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// Roles configura os nomes dos papéis.
type Roles struct {
	Provider string
	Operator string
}

// IsProvider indica um provedor de jogos com providerId vinculado.
func (r Roles) IsProvider(p Principal) bool { return p.HasRole(r.Provider) && p.ProviderID != "" }

// IsOperator indica o serviço interno de carteiras.
func (r Roles) IsOperator(p Principal) bool { return p.HasRole(r.Operator) }

// Authenticator valida tokens JWT: assinatura (JWKS do IdP, apenas RS256),
// emissor, audiência, expiração e tipo "Bearer".
type Authenticator struct {
	verifier      *oidc.IDTokenVerifier
	providerClaim string
	Roles         Roles
}

// NewAuthenticatorWithKeySet cria o autenticador com um conjunto de chaves
// explícito (usado em testes unitários com chaves locais).
func NewAuthenticatorWithKeySet(issuer, audience, providerClaim string, roles Roles, ks oidc.KeySet, now func() time.Time) *Authenticator {
	return &Authenticator{
		verifier: oidc.NewVerifier(issuer, ks, &oidc.Config{
			ClientID:             audience,
			SupportedSigningAlgs: []string{oidc.RS256},
			Now:                  now,
		}),
		providerClaim: providerClaim,
		Roles:         roles,
	}
}

// NewAuthenticator cria o autenticador com as chaves remotas do IdP. No início
// da aplicação o endpoint JWKS é consultado para validar a dependência.
func NewAuthenticator(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) *Authenticator {
	ks := oidc.NewRemoteKeySet(context.Background(), cfg.OIDCJWKSURL)
	a := NewAuthenticatorWithKeySet(cfg.OIDCIssuerURL, cfg.OIDCAudience, cfg.OIDCProviderClaim,
		Roles{Provider: cfg.ProviderRole, Operator: cfg.OperatorRole}, ks, nil)
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		return waitJWKS(ctx, cfg.OIDCJWKSURL, log)
	}})
	return a
}

func waitJWKS(ctx context.Context, url string, log *slog.Logger) error {
	client := &http.Client{Timeout: 3 * time.Second}
	return startup.Retry(ctx, log, "oidc-jwks", func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status %d", resp.StatusCode)
		}
		return nil
	})
}

type accessClaims struct {
	Type        string `json:"typ"`
	AZP         string `json:"azp"`
	ClientID    string `json:"client_id"`
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// Authenticate valida o token bruto e devolve o principal.
func (a *Authenticator) Authenticate(ctx context.Context, raw string) (Principal, error) {
	if raw == "" {
		return Principal{}, ErrMissingToken
	}
	tok, err := a.verifier.Verify(ctx, raw)
	if err != nil {
		var expired *oidc.TokenExpiredError
		if errors.As(err, &expired) {
			return Principal{}, ErrExpiredToken
		}
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	var claims accessClaims
	if err := tok.Claims(&claims); err != nil {
		return Principal{}, fmt.Errorf("%w: claims ilegíveis", ErrInvalidToken)
	}
	// Apenas access tokens: impede o uso de ID tokens ou refresh tokens.
	if !strings.EqualFold(claims.Type, "Bearer") {
		return Principal{}, fmt.Errorf("%w: tipo de token %q", ErrInvalidToken, claims.Type)
	}
	var extra map[string]json.RawMessage
	if err := tok.Claims(&extra); err != nil {
		return Principal{}, fmt.Errorf("%w: claims ilegíveis", ErrInvalidToken)
	}
	var providerID string
	if raw, ok := extra[a.providerClaim]; ok {
		if err := json.Unmarshal(raw, &providerID); err != nil {
			return Principal{}, fmt.Errorf("%w: claim %s inválido", ErrInvalidToken, a.providerClaim)
		}
	}
	clientID := claims.AZP
	if clientID == "" {
		clientID = claims.ClientID
	}
	return Principal{Subject: tok.Subject, ClientID: clientID, ProviderID: providerID, Roles: claims.RealmAccess.Roles}, nil
}

type ctxKey struct{}

// WithPrincipal adiciona o principal ao contexto.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// FromContext obtém o principal do contexto.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}

// BearerToken extrai o token do header Authorization.
func BearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const prefix = "bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(h[len(prefix):])
}

// Module registra o autenticador no Fx; o Invoke valida o IdP logo após o banco.
var Module = fx.Module("auth", fx.Provide(NewAuthenticator), fx.Invoke(func(*Authenticator) {}))
