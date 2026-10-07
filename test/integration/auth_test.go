//go:build integration

package integration

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAuthenticationWithRealIdP(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	inst := startInstance(t, "auth-1", baseEnv(q))
	w := openWallet(t, inst, "100.00")
	body := wagerBody(w, "provider-a", "auth-"+uuid.NewString(), "BET", "10.00", "")

	expired := token(t, "provider-a-short-lived") // tokens com 1s de validade
	time.Sleep(2500 * time.Millisecond)

	valid := token(t, "provider-a")
	parts := strings.Split(valid, ".")
	tampered := parts[0] + "." + strings.TrimRight(parts[1], "A") + "B." + parts[2]

	// Token do realm master (outro emissor, outra chave).
	form := url.Values{"grant_type": {"password"}, "client_id": {"admin-cli"}, "username": {"admin"}, "password": {"admin"}}
	resp, err := http.PostForm(keycloakURL+"/realms/master/protocol/openid-connect/token", form)
	if err != nil {
		t.Fatal(err)
	}
	var master struct {
		AccessToken string `json:"access_token"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&master)
	resp.Body.Close()
	if master.AccessToken == "" {
		t.Fatal("não foi possível obter token do realm master")
	}

	cases := map[string]string{
		"ausente":             "",
		"lixo":                "not-a-jwt",
		"expirado":            expired,
		"assinatura inválida": tampered,
		"audiência errada":    token(t, "foreign-audience"),
		"outro emissor":       master.AccessToken,
	}
	for name, tok := range cases {
		r := call(t, "POST", inst.Base+"/wagering/transactions", tok, body, map[string]string{"Idempotency-Key": "k-" + name})
		if r.Status != http.StatusUnauthorized || r.str("error", "code") != "UNAUTHENTICATED" {
			t.Errorf("%s: status %d %s", name, r.Status, r.Raw)
		}
	}
	if n := queryInt(t, `SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1 AND origin = 'EXTERNAL'`, w.ID); n != 0 {
		t.Fatalf("acessos não autenticados geraram %d transações", n)
	}
	if !strings.Contains(inst.Logs(), `"reason":"expired_token"`) {
		t.Error("expiração deveria ser registrada como expired_token")
	}
}

func TestAuthorizationAndProviderIsolation(t *testing.T) {
	q := createQueues(t, 3, 5*time.Second)
	inst := startInstance(t, "authz-1", baseEnv(q))
	w := openWallet(t, inst, "100.00")
	extA := "iso-" + uuid.NewString()

	// provider-a cria uma aposta.
	a := submit(t, inst, "provider-a", wagerBody(w, "provider-a", extA, "BET", "10.00", ""), "provider-a:"+extA)
	if a.Status != http.StatusCreated {
		t.Fatalf("aposta de provider-a: %d %s", a.Status, a.Raw)
	}
	txA := a.str("transactionId")

	type check struct {
		name, method, path, client string
		body                       any
		headers                    map[string]string
		want                       int
	}
	checks := []check{
		// Operações de carteira são restritas ao serviço interno.
		{"provider abre carteira", "POST", "/wallets", "provider-a", map[string]any{"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": "1.00", "currency": "BRL"}}, nil, 403},
		{"provider lê carteira", "GET", "/wallets/" + w.ID, "provider-a", nil, nil, 403},
		{"provider lê ledger", "GET", "/wallets/" + w.ID + "/ledger", "provider-b", nil, nil, 403},
		{"provider reconcilia", "POST", "/wallets/" + w.ID + "/reconciliation", "provider-a", nil, nil, 403},
		{"sem papel lê carteira", "GET", "/wallets/" + w.ID, "observer", nil, nil, 403},
		// Envio de operações é restrito a provedores e ao próprio providerId.
		{"serviço interno envia operação", "POST", "/wagering/transactions", "wallet-service", wagerBody(w, "provider-a", "x-"+uuid.NewString(), "BET", "1.00", ""), map[string]string{"Idempotency-Key": "k1"}, 403},
		{"sem papel envia operação", "POST", "/wagering/transactions", "observer", wagerBody(w, "provider-a", "x-"+uuid.NewString(), "BET", "1.00", ""), map[string]string{"Idempotency-Key": "k2"}, 403},
		{"provider-a se passa por provider-b", "POST", "/wagering/transactions", "provider-a", wagerBody(w, "provider-b", "x-"+uuid.NewString(), "BET", "1.00", ""), map[string]string{"Idempotency-Key": "k3"}, 403},
		{"provider-b replay da chave de provider-a com providerId de A", "POST", "/wagering/transactions", "provider-b", wagerBody(w, "provider-a", extA, "BET", "10.00", ""), map[string]string{"Idempotency-Key": "provider-a:" + extA}, 403},
		// Consultas: provedores só enxergam as próprias transações.
		{"provider-b lê transação de A por id", "GET", "/wagering/transactions/" + txA, "provider-b", nil, nil, 404},
		{"provider-b lê transação de A por id externo", "GET", "/providers/provider-a/wagering/transactions/" + extA, "provider-b", nil, nil, 403},
		{"sem papel lê transação", "GET", "/wagering/transactions/" + txA, "observer", nil, nil, 403},
		// Acessos permitidos.
		{"provider-a lê a própria transação", "GET", "/wagering/transactions/" + txA, "provider-a", nil, nil, 200},
		{"provider-a lê por id externo", "GET", "/providers/provider-a/wagering/transactions/" + extA, "provider-a", nil, nil, 200},
		{"serviço interno lê qualquer transação", "GET", "/wagering/transactions/" + txA, "wallet-service", nil, nil, 200},
	}
	for _, c := range checks {
		r := call(t, c.method, inst.Base+c.path, token(t, c.client), c.body, c.headers)
		if r.Status != c.want {
			t.Errorf("%s: status %d (esperado %d) %s", c.name, r.Status, c.want, r.Raw)
		}
		if c.want >= 400 && strings.Contains(r.Raw, w.PlayerID) {
			t.Errorf("%s: resposta de erro expõe dados da carteira", c.name)
		}
	}

	// provider-b usando a mesma chave/ID externo no próprio escopo cria operação
	// independente e nunca recebe o resultado de provider-a.
	b := submit(t, inst, "provider-b", wagerBody(w, "provider-b", extA, "BET", "5.00", ""), "provider-a:"+extA)
	if b.Status != http.StatusCreated || b.str("transactionId") == txA || b.Body["idempotentReplay"] != false {
		t.Fatalf("escopo por provedor: %d %s", b.Status, b.Raw)
	}

	// Nenhum efeito financeiro dos acessos negados: apenas as duas apostas legítimas.
	if bal := walletBalance(t, w.ID); bal != 8500 {
		t.Fatalf("saldo = %d, esperado 8500", bal)
	}
	if n := queryInt(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, w.ID); n != 3 {
		t.Fatalf("lançamentos = %d, esperado 3", n)
	}
	if inst.Metric("auth_failures_total") < 10 {
		t.Error("falhas de autorização deveriam ser contabilizadas")
	}
}
