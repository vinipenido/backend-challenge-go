//go:build integration

// Package integration contém os testes de integração com PostgreSQL, Keycloak e
// LocalStack reais, executando múltiplas instâncias independentes do serviço
// (processos próprios, com conexões e memória próprias).
//
// Pré-requisito: docker compose up -d postgres keycloak localstack
// Execução:      go test -tags integration -race -count=1 -p 1 ./test/integration/...
package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/golang-migrate/migrate/v4"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vinipenido/backend-challenge-go/internal/migrator"
)

// env lê variáveis de ambiente com padrão compatível com o docker compose.
func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

var (
	pgHost      = env("IT_PG_HOSTPORT", "localhost:5432")
	pgDB        = env("IT_PG_DATABASE", "wallet_test")
	adminURL    = fmt.Sprintf("postgres://postgres:postgres@%s/%s?sslmode=disable", pgHost, pgDB)
	ownerURL    = fmt.Sprintf("postgres://wallet_owner:wallet_owner@%s/%s?sslmode=disable", pgHost, pgDB)
	appUser     = "wallet_app:wallet_app"
	keycloakURL = env("IT_KEYCLOAK_URL", "http://localhost:8080")
	sqsEndpoint = env("IT_SQS_ENDPOINT", "http://localhost:4566")
	issuer      = env("IT_OIDC_ISSUER", "http://keycloak:8080/realms/wagering")

	binaryPath string
	adminPool  *pgxpool.Pool
	sqsClient  *sqs.Client
)

func appURL(hostport string) string {
	return fmt.Sprintf("postgres://%s@%s/%s?sslmode=disable", appUser, hostport, pgDB)
}

func TestMain(m *testing.M) {
	code, err := setup(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "setup da integração falhou:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func setup(m *testing.M) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// 1. Schema limpo: reverte tudo e reaplica (valida as duas direções).
	mg, err := migrator.New(ownerURL)
	if err != nil {
		return 0, err
	}
	if err := mg.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return 0, fmt.Errorf("migrate down: %w", err)
	}
	if err := mg.Up(); err != nil {
		return 0, fmt.Errorf("migrate up: %w", err)
	}
	mg.Close()

	adminPool, err = pgxpool.New(ctx, adminURL)
	if err != nil {
		return 0, err
	}
	defer adminPool.Close()

	// 2. Cliente SQS (credenciais locais de teste).
	sqsClient = sqs.New(sqs.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(sqsEndpoint),
		Credentials:  credentials.NewStaticCredentialsProvider("integration-tests", "local", ""),
	})
	if _, err := sqsClient.ListQueues(ctx, &sqs.ListQueuesInput{}); err != nil {
		return 0, fmt.Errorf("LocalStack indisponível: %w", err)
	}
	if _, err := fetchToken(ctx, "provider-a", "provider-a-secret"); err != nil {
		return 0, fmt.Errorf("Keycloak indisponível: %w", err)
	}

	// 3. Binário com injeção de falhas (build tag faultinject).
	dir, err := os.MkdirTemp("", "wallet-it-*")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(dir)
	binaryPath = filepath.Join(dir, "wallet-service")
	args := []string{"build", "-tags", "faultinject", "-o", binaryPath}
	if os.Getenv("IT_BINARY_RACE") == "1" {
		args = append(args, "-race")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..")
	build := exec.Command("go", append(args, "./cmd/wallet-service")...)
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		return 0, fmt.Errorf("go build: %v\n%s", err, out)
	}
	return m.Run(), nil
}

// ---------------------------------------------------------------------------
// Tokens do IdP
// ---------------------------------------------------------------------------

var (
	tokenMu    sync.Mutex
	tokenCache = map[string]cachedToken{}
)

type cachedToken struct {
	value string
	exp   time.Time
}

func fetchToken(ctx context.Context, clientID, secret string) (string, error) {
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {secret}}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, keycloakURL+"/realms/wagering/protocol/openid-connect/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.AccessToken == "" {
		return "", fmt.Errorf("token de %s indisponível (status %d)", clientID, resp.StatusCode)
	}
	return out.AccessToken, nil
}

// token devolve um access token real do Keycloak (com cache até perto da expiração).
func token(t *testing.T, clientID string) string {
	t.Helper()
	tokenMu.Lock()
	defer tokenMu.Unlock()
	if c, ok := tokenCache[clientID]; ok && time.Until(c.exp) > 30*time.Second {
		return c.value
	}
	v, err := fetchToken(context.Background(), clientID, clientID+"-secret")
	if err != nil {
		t.Fatal(err)
	}
	tokenCache[clientID] = cachedToken{value: v, exp: time.Now().Add(4 * time.Minute)}
	return v
}

// ---------------------------------------------------------------------------
// Filas isoladas por teste
// ---------------------------------------------------------------------------

type queues struct {
	Wager, DLQ, Events          string // nomes
	WagerURL, DLQURL, EventsURL string
}

func createQueues(t *testing.T, maxReceive int, visibility time.Duration) queues {
	t.Helper()
	ctx := context.Background()
	prefix := "it-" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	q := queues{Wager: prefix + "-wager.fifo", DLQ: prefix + "-dlq.fifo", Events: prefix + "-events.fifo"}
	create := func(name string, attrs map[string]string) string {
		base := map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false"}
		for k, v := range attrs {
			base[k] = v
		}
		out, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: base})
		if err != nil {
			t.Fatalf("criar fila %s: %v", name, err)
		}
		return aws.ToString(out.QueueUrl)
	}
	q.DLQURL = create(q.DLQ, nil)
	redrive, _ := json.Marshal(map[string]string{
		"deadLetterTargetArn": "arn:aws:sqs:us-east-1:000000000000:" + q.DLQ,
		"maxReceiveCount":     strconv.Itoa(maxReceive),
	})
	q.WagerURL = create(q.Wager, map[string]string{
		"VisibilityTimeout": strconv.Itoa(int(visibility / time.Second)),
		"RedrivePolicy":     string(redrive),
	})
	q.EventsURL = create(q.Events, nil)
	t.Cleanup(func() {
		for _, u := range []string{q.WagerURL, q.DLQURL, q.EventsURL} {
			_, _ = sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(u)})
		}
	})
	return q
}

func sendSQS(t *testing.T, queueURL, body, group string) {
	t.Helper()
	_, err := sqsClient.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: aws.String(queueURL), MessageBody: aws.String(body), MessageGroupId: aws.String(group),
		MessageDeduplicationId: aws.String(uuid.NewString()),
	})
	if err != nil {
		t.Fatal(err)
	}
}

// drainQueue lê todas as mensagens disponíveis (removendo-as) até não haver mais.
func drainQueue(t *testing.T, queueURL string, idle time.Duration) []types.Message {
	t.Helper()
	var out []types.Message
	deadline := time.Now().Add(idle)
	for time.Now().Before(deadline) {
		res, err := sqsClient.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1,
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range res.Messages {
			out = append(out, m)
			_, _ = sqsClient.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(queueURL), ReceiptHandle: m.ReceiptHandle})
		}
		if len(res.Messages) > 0 {
			deadline = time.Now().Add(idle)
		}
	}
	return out
}

func queueDepth(t *testing.T, queueURL string) (visible, inFlight int) {
	t.Helper()
	out, err := sqsClient.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	visible, _ = strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])
	inFlight, _ = strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
	return
}

// ---------------------------------------------------------------------------
// Instâncias (processos independentes)
// ---------------------------------------------------------------------------

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type instance struct {
	t      *testing.T
	Name   string
	Base   string
	cmd    *exec.Cmd
	logs   *syncBuffer
	exited chan struct{}
	err    error
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// baseEnv devolve a configuração padrão de uma instância de teste.
func baseEnv(q queues) map[string]string {
	return map[string]string{
		"APP_ENV": "test", "LOG_LEVEL": "debug",
		"DATABASE_URL":    appURL(pgHost),
		"OIDC_ISSUER_URL": issuer,
		"OIDC_JWKS_URL":   keycloakURL + "/realms/wagering/protocol/openid-connect/certs",
		"OIDC_AUDIENCE":   "wallet-api",
		"AWS_REGION":      "us-east-1", "AWS_ENDPOINT_URL": sqsEndpoint,
		"AWS_ACCESS_KEY_ID": "wallet-service", "AWS_SECRET_ACCESS_KEY": "local",
		"SQS_WAGER_QUEUE_NAME": q.Wager, "SQS_WAGER_DLQ_NAME": q.DLQ, "SQS_EVENTS_QUEUE_NAME": q.Events,
		"SQS_TRUSTED_PROVIDERS": "provider-a,provider-b",
		"SQS_WAIT_TIME":         "1s", "SQS_VISIBILITY_TIMEOUT": "5s", "SQS_PROCESSING_TIMEOUT": "4s",
		"SQS_RETRY_BASE": "1s", "SQS_RETRY_MAX": "2s",
		"OUTBOX_POLL_INTERVAL": "100ms", "OUTBOX_LEASE": "3s", "OUTBOX_RETRY_BASE": "500ms", "OUTBOX_RETRY_MAX": "2s",
		"PENDING_POLL_INTERVAL": "100ms", "PENDING_BACKOFF_BASE": "200ms", "PENDING_BACKOFF_MAX": "1s",
		"PENDING_REFERENCE_TTL": "1m", "PENDING_REFERENCE_MAX_ATTEMPTS": "100",
		"SHUTDOWN_TIMEOUT": "15s", "START_TIMEOUT": "30s", "DATABASE_LOCK_TIMEOUT": "5s",
	}
}

// startInstance inicia o binário como processo próprio e aguarda o readiness.
func startInstance(t *testing.T, name string, envs map[string]string) *instance {
	t.Helper()
	inst := launch(t, name, envs)
	inst.waitReady(30 * time.Second)
	return inst
}

func launch(t *testing.T, name string, envs map[string]string) *instance {
	t.Helper()
	port := freePort(t)
	envs["HTTP_ADDR"] = fmt.Sprintf("127.0.0.1:%d", port)
	envs["INSTANCE_ID"] = name
	cmd := exec.Command(binaryPath)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
	for k, v := range envs {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	logs := &syncBuffer{}
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	inst := &instance{t: t, Name: name, Base: fmt.Sprintf("http://127.0.0.1:%d", port), cmd: cmd, logs: logs, exited: make(chan struct{})}
	go func() {
		inst.err = cmd.Wait()
		close(inst.exited)
	}()
	t.Cleanup(func() {
		inst.Kill()
		if t.Failed() {
			t.Logf("=== logs de %s (final) ===\n%s", name, tail(logs.String(), 60))
		}
	})
	return inst
}

func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

func (i *instance) waitReady(timeout time.Duration) {
	i.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-i.exited:
			i.t.Fatalf("%s terminou antes de ficar pronto: %v\n%s", i.Name, i.err, tail(i.logs.String(), 40))
		default:
		}
		resp, err := http.Get(i.Base + "/health/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	i.t.Fatalf("%s não ficou pronto em %s\n%s", i.Name, timeout, tail(i.logs.String(), 40))
}

// Kill encerra abruptamente (SIGKILL), simulando queda do processo.
func (i *instance) Kill() {
	select {
	case <-i.exited:
		return
	default:
	}
	_ = i.cmd.Process.Signal(syscall.SIGKILL)
	<-i.exited
}

// Terminate envia SIGTERM e aguarda a saída; devolve o erro de saída.
func (i *instance) Terminate(timeout time.Duration) error {
	_ = i.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-i.exited:
		return i.err
	case <-time.After(timeout):
		i.Kill()
		return fmt.Errorf("%s não terminou em %s após SIGTERM", i.Name, timeout)
	}
}

// WaitExit aguarda o término espontâneo (ex.: crash injetado).
func (i *instance) WaitExit(timeout time.Duration) bool {
	select {
	case <-i.exited:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (i *instance) Logs() string { return i.logs.String() }

// Metric soma os valores de uma métrica Prometheus cujas linhas contenham todos os filtros.
func (i *instance) Metric(name string, filters ...string) float64 {
	i.t.Helper()
	resp, err := http.Get(i.Base + "/metrics")
	if err != nil {
		i.t.Fatal(err)
	}
	defer resp.Body.Close()
	var total float64
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, name+"{") && !strings.HasPrefix(line, name+" ") {
			continue
		}
		ok := true
		for _, f := range filters {
			if !strings.Contains(line, f) {
				ok = false
			}
		}
		if !ok {
			continue
		}
		fields := strings.Fields(line)
		v, _ := strconv.ParseFloat(fields[len(fields)-1], 64)
		total += v
	}
	return total
}

// ---------------------------------------------------------------------------
// Cliente HTTP
// ---------------------------------------------------------------------------

type response struct {
	Status int
	Body   map[string]any
	Raw    string
}

func (r response) str(path ...string) string {
	var cur any = r.Body
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[p]
	}
	s, _ := cur.(string)
	return s
}

func call(t *testing.T, method, url, bearer string, body any, headers map[string]string) response {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		switch b := body.(type) {
		case string:
			rdr = strings.NewReader(b)
		default:
			raw, _ := json.Marshal(b)
			rdr = bytes.NewReader(raw)
		}
	}
	req, _ := http.NewRequest(method, url, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := response{Status: resp.StatusCode, Raw: string(raw)}
	_ = json.Unmarshal(raw, &out.Body)
	return out
}

type wallet struct {
	ID, PlayerID string
}

func openWallet(t *testing.T, inst *instance, amount string) wallet {
	t.Helper()
	player := uuid.NewString()
	r := call(t, "POST", inst.Base+"/wallets", token(t, "wallet-service"), map[string]any{
		"playerId": player, "initialBalance": map[string]string{"amount": amount, "currency": "BRL"},
	}, nil)
	if r.Status != http.StatusCreated {
		t.Fatalf("abrir carteira: %d %s", r.Status, r.Raw)
	}
	return wallet{ID: r.str("id"), PlayerID: player}
}

func wagerBody(w wallet, provider, ext, kind, amount, ref string) map[string]any {
	b := map[string]any{
		"providerId": provider, "externalTransactionId": ext, "playerId": w.PlayerID, "walletId": w.ID,
		"roundId": "round-1", "gameId": "game-1", "kind": kind,
		"money": map[string]string{"amount": amount, "currency": "BRL"},
	}
	if ref != "" {
		b["referenceExternalTransactionId"] = ref
	}
	return b
}

func submit(t *testing.T, inst *instance, client string, body map[string]any, key string) response {
	t.Helper()
	return call(t, "POST", inst.Base+"/wagering/transactions", token(t, client), body, map[string]string{"Idempotency-Key": key})
}

func sqsMessage(messageID string, w wallet, provider, ext, kind, amount, ref, key string) string {
	data := wagerBody(w, provider, ext, kind, amount, ref)
	data["idempotencyKey"] = key
	raw, _ := json.Marshal(map[string]any{
		"messageId": messageID, "type": "WagerTransactionRequested",
		"occurredAt": time.Now().UTC().Format(time.RFC3339Nano), "data": data,
	})
	return string(raw)
}

// ---------------------------------------------------------------------------
// Consultas diretas ao banco (verificação)
// ---------------------------------------------------------------------------

func queryInt(t *testing.T, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := adminPool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func walletBalance(t *testing.T, walletID string) int64 {
	return queryInt(t, `SELECT balance_minor FROM wallets WHERE id = $1`, walletID)
}

// assertLedgerConsistent confere saldo armazenado = créditos - débitos do ledger.
func assertLedgerConsistent(t *testing.T, walletID string) {
	t.Helper()
	stored := walletBalance(t, walletID)
	calc := queryInt(t, `SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount_minor ELSE -amount_minor END), 0)::BIGINT
		FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID)
	if stored != calc {
		t.Fatalf("carteira %s: saldo armazenado %d != ledger %d", walletID, stored, calc)
	}
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("condição não atingida em %s: %s", timeout, what)
}

// ---------------------------------------------------------------------------
// Proxy TCP para simular indisponibilidade de dependências
// ---------------------------------------------------------------------------

type tcpProxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	up     bool
	conns  map[net.Conn]struct{}
}

func newProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{ln: ln, target: target, up: true, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(func() { ln.Close(); p.SetUp(false) })
	return p
}

func (p *tcpProxy) Addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		if !p.up {
			p.mu.Unlock()
			c.Close()
			continue
		}
		up, err := net.DialTimeout("tcp", p.target, 2*time.Second)
		if err != nil {
			p.mu.Unlock()
			c.Close()
			continue
		}
		p.conns[c], p.conns[up] = struct{}{}, struct{}{}
		p.mu.Unlock()
		go func() { _, _ = io.Copy(up, c); up.Close(); c.Close() }()
		go func() { _, _ = io.Copy(c, up); up.Close(); c.Close() }()
	}
}

// SetUp liga/desliga o proxy; desligar derruba as conexões abertas.
func (p *tcpProxy) SetUp(up bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.up = up
	if !up {
		for c := range p.conns {
			c.Close()
		}
		p.conns = map[net.Conn]struct{}{}
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
