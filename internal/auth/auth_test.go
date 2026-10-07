package auth_test

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"

	"github.com/vinipenido/backend-challenge-go/internal/auth"
)

const issuer = "http://idp.test/realms/wagering"

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func sign(t *testing.T, key *rsa.PrivateKey, claims map[string]any) string {
	t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key}, (&jose.SignerOptions{}).WithType("JWT"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := jwt.Signed(signer).Claims(claims).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func baseClaims() map[string]any {
	return map[string]any{
		"iss": issuer, "aud": "wallet-api", "sub": "svc-provider-a", "azp": "provider-a", "typ": "Bearer",
		"exp": now.Add(5 * time.Minute).Unix(), "iat": now.Unix(), "provider_id": "provider-a",
		"realm_access": map[string]any{"roles": []string{"wagering-provider"}},
	}
}

func newAuth(key *rsa.PrivateKey) *auth.Authenticator {
	return auth.NewAuthenticatorWithKeySet(issuer, "wallet-api", "provider_id",
		auth.Roles{Provider: "wagering-provider", Operator: "wallet-operator"},
		&oidc.StaticKeySet{PublicKeys: []crypto.PublicKey{key.Public()}}, func() time.Time { return now })
}

func TestAuthenticateValidToken(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	a := newAuth(key)
	p, err := a.Authenticate(context.Background(), sign(t, key, baseClaims()))
	if err != nil {
		t.Fatal(err)
	}
	if p.ProviderID != "provider-a" || p.ClientID != "provider-a" || !a.Roles.IsProvider(p) || a.Roles.IsOperator(p) {
		t.Fatalf("principal = %+v", p)
	}
}

func TestAuthenticateRejections(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	a := newAuth(key)

	mutate := func(f func(map[string]any)) string {
		c := baseClaims()
		f(c)
		return sign(t, key, c)
	}
	cases := map[string]struct {
		token string
		want  error
	}{
		"ausente":                {"", auth.ErrMissingToken},
		"malformado":             {"abc.def.ghi", auth.ErrInvalidToken},
		"outra chave":            {sign(t, other, baseClaims()), auth.ErrInvalidToken},
		"expirado":               {mutate(func(c map[string]any) { c["exp"] = now.Add(-time.Second).Unix() }), auth.ErrExpiredToken},
		"emissor diferente":      {mutate(func(c map[string]any) { c["iss"] = "http://evil.test" }), auth.ErrInvalidToken},
		"audiência errada":       {mutate(func(c map[string]any) { c["aud"] = "account" }), auth.ErrInvalidToken},
		"ID token":               {mutate(func(c map[string]any) { c["typ"] = "ID" }), auth.ErrInvalidToken},
		"provider_id não-string": {mutate(func(c map[string]any) { c["provider_id"] = 42 }), auth.ErrInvalidToken},
	}
	for name, tc := range cases {
		if _, err := a.Authenticate(context.Background(), tc.token); !errors.Is(err, tc.want) {
			t.Errorf("%s: esperado %v, obtido %v", name, tc.want, err)
		}
	}

	// Token adulterado: payload trocado mantendo a assinatura original.
	good := sign(t, key, baseClaims())
	evil := sign(t, key, mutate2(baseClaims()))
	tampered := good[:indexDot(good)] + evil[indexDot(evil):lastDot(evil)] + good[lastDot(good):]
	if _, err := a.Authenticate(context.Background(), tampered); !errors.Is(err, auth.ErrInvalidToken) {
		t.Errorf("adulterado: %v", err)
	}
}

func mutate2(c map[string]any) map[string]any {
	c["provider_id"] = "provider-b"
	return c
}

func indexDot(s string) int {
	for i := range s {
		if s[i] == '.' {
			return i
		}
	}
	return -1
}

func lastDot(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '.' {
			return i
		}
	}
	return -1
}

func TestRolesAndBearer(t *testing.T) {
	roles := auth.Roles{Provider: "wagering-provider", Operator: "wallet-operator"}
	if roles.IsProvider(auth.Principal{Roles: []string{"wagering-provider"}}) {
		t.Fatal("papel de provedor sem provider_id não deve ser aceito")
	}
	if !roles.IsOperator(auth.Principal{Roles: []string{"wallet-operator"}}) {
		t.Fatal("operador")
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer abc")
	if auth.BearerToken(r) != "abc" {
		t.Fatal("bearer")
	}
	r.Header.Set("Authorization", "Basic abc")
	if auth.BearerToken(r) != "" {
		t.Fatal("basic não é bearer")
	}
}
