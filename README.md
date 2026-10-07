# Wallet Service — Processamento Distribuído de Apostas em Go

Serviço em **Go 1.27.1** com **Uber Fx** que movimenta carteiras de jogadores a partir de operações de provedores de jogos (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`), recebidas por **HTTP** e por **AWS SQS** com as mesmas garantias: dinheiro exato (sem ponto flutuante), ledger append-only, idempotência persistente, coordenação por carteira entre várias instâncias, inbox/outbox transacionais e recuperação de falhas.

- Enunciado original do desafio: [`docs/DESAFIO.md`](docs/DESAFIO.md)
- Decisões técnicas, limitações e interpretações: [`ARCHITECTURE.md`](ARCHITECTURE.md)

```
              ┌──────────── Keycloak (OIDC, client_credentials) ───────────┐
              │ JWKS                                                       │ tokens
 provedores ──┼──► HTTP ──┐                                                │
              │           ├──► caso de uso único ──► PostgreSQL ◄── workers (pendências, outbox)
 provedores ──┴──► SQS ───┘   (lock por carteira,   (saldo, ledger,          │
   wager-transactions.fifo     idempotência, inbox)  inbox, outbox)          ▼
        └─► wager-transactions-dlq.fifo                             wallet-events.fifo
```

## 1. Pré-requisitos

| Ferramenta | Versão usada | Uso |
| --- | --- | --- |
| Docker + Docker Compose v2 | Docker 29 / Compose 5 | ambiente completo e dependências dos testes |
| Go | 1.27.1 (mesma do `go.mod` e do `Dockerfile`) | testes e execução no host |
| `curl`, `jq`, `uuidgen` | — | scripts de demonstração |
| AWS CLI (opcional) | v2 | enviar mensagens SQS manualmente |

Portas usadas no host: `5432` (PostgreSQL), `8080` (Keycloak), `4566` (LocalStack), `8081`–`8083` (instâncias do serviço).

## 2. Subindo tudo

```sh
docker compose up --build
```

O Compose sobe, nesta ordem:

1. **postgres** (`postgres:16-alpine`) — cria os papéis `wallet_owner` (dono do schema, usado só pelas migrations) e `wallet_app` (privilégios mínimos) e os bancos `wallet` e `wallet_test` (`deploy/postgres/init.sql`);
2. **keycloak** (`26.0`) — importa automaticamente o realm `wagering` com clientes, papéis e identidades de teste (`deploy/keycloak/realm-wagering.json`);
3. **localstack** (`3.8`) — provisiona automaticamente as filas FIFO, a redrive policy e as políticas IAM (`deploy/localstack/init-aws.sh`);
4. **migrate** — aplica as migrations (`/app/migrate up`) e termina;
5. **wallet-1, wallet-2, wallet-3** — três instâncias independentes (processos, conexões e memória próprios) em `localhost:8081`, `8082` e `8083`, cada uma com API HTTP, consumidor SQS, publicador da outbox e worker de pendências.

Quando as três instâncias estiverem `healthy` (`docker compose ps`):

```sh
./scripts/demo.sh                 # fluxos autenticados completos (instância 8081)
BASE_URL=http://localhost:8083 ./scripts/demo.sh
./scripts/concurrency-demo.sh     # duas apostas de 80.00 sobre 100.00 em instâncias diferentes
```

Para derrubar e apagar os dados: `docker compose down -v`.

## 3. Identidades de teste (Keycloak)

Realm `wagering`, emissor `http://keycloak:8080/realms/wagering` (fixado por `KC_HOSTNAME`, inclusive para tokens obtidos pelo host em `localhost:8080`). Todos os clientes usam `client_credentials`; os segredos abaixo são valores **locais de exemplo**.

| client_id | secret | Papel (realm role) | Claim `provider_id` | Uso |
| --- | --- | --- | --- | --- |
| `provider-a` | `provider-a-secret` | `wagering-provider` | `provider-a` | provedor A |
| `provider-b` | `provider-b-secret` | `wagering-provider` | `provider-b` | provedor B |
| `wallet-service` | `wallet-service-secret` | `wallet-operator` | — | serviço interno (carteiras, ledger, reconciliação) |
| `provider-a-short-lived` | `provider-a-short-lived-secret` | `wagering-provider` | `provider-a` | tokens de 1 s (teste de expiração) |
| `observer` | `observer-secret` | — | — | autenticado sem papel (teste de 403) |
| `foreign-audience` | `foreign-audience-secret` | `wagering-provider` | `provider-a` | token sem audiência `wallet-api` (teste de 401) |

Console administrativo: <http://localhost:8080> (`admin` / `admin`).

```sh
TOKEN=$(curl -s -d grant_type=client_credentials -d client_id=provider-a -d client_secret=provider-a-secret \
  http://localhost:8080/realms/wagering/protocol/openid-connect/token | jq -r .access_token)
```

## 4. Variáveis de ambiente

Todas têm valor padrão, exceto as marcadas como obrigatórias. O arquivo [`.env.example`](.env.example) contém valores locais para executar no host.

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `INSTANCE_ID` | hostname | identificação da instância (logs, leases da outbox) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `HTTP_ADDR` | `:8080` | endereço da API |
| `HTTP_REQUEST_TIMEOUT` | `10s` | prazo máximo por requisição |
| `DATABASE_URL` | **obrigatória** | conexão com o papel `wallet_app` |
| `DATABASE_MAX_CONNS` | `10` | tamanho do pool |
| `DATABASE_LOCK_TIMEOUT` / `DATABASE_STATEMENT_TIMEOUT` | `5s` / `15s` | proteções por sessão |
| `MIGRATIONS_DATABASE_URL` | — | conexão do binário `migrate` (papel `wallet_owner`) |
| `OIDC_ISSUER_URL` | **obrigatória** | emissor esperado (`iss`) |
| `OIDC_JWKS_URL` | `<issuer>/protocol/openid-connect/certs` | onde buscar as chaves públicas |
| `OIDC_AUDIENCE` | `wallet-api` | audiência exigida (`aud`) |
| `OIDC_PROVIDER_CLAIM` | `provider_id` | claim que define o provedor autenticado |
| `OIDC_PROVIDER_ROLE` / `OIDC_OPERATOR_ROLE` | `wagering-provider` / `wallet-operator` | papéis |
| `AWS_REGION` | `us-east-1` | região |
| `AWS_ENDPOINT_URL` | — | endpoint do LocalStack (vazio = AWS real) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | cadeia padrão AWS | credenciais do papel `wallet-service` no broker |
| `SQS_WAGER_QUEUE_NAME` / `SQS_WAGER_DLQ_NAME` / `SQS_EVENTS_QUEUE_NAME` | `wager-transactions.fifo` / `wager-transactions-dlq.fifo` / `wallet-events.fifo` | filas |
| `SQS_TRUSTED_PROVIDERS` | **obrigatória com consumidor ativo** | provedores aceitos pela fila de entrada |
| `SQS_CONSUMER_ENABLED`, `OUTBOX_PUBLISHER_ENABLED`, `PENDING_WORKER_ENABLED`, `HTTP_ENABLED` | `true` | liga/desliga componentes |
| `SQS_POLLERS`, `SQS_WAIT_TIME`, `SQS_MAX_MESSAGES` | `2`, `5s`, `10` | long polling |
| `SQS_VISIBILITY_TIMEOUT`, `SQS_PROCESSING_TIMEOUT` | `30s`, `15s` | prazo de processamento < visibility timeout |
| `SQS_RETRY_BASE`, `SQS_RETRY_MAX` | `2s`, `60s` | backoff de falhas transitórias (via visibilidade) |
| `OUTBOX_POLL_INTERVAL`, `OUTBOX_BATCH_SIZE`, `OUTBOX_LEASE` | `500ms`, `50`, `30s` | publicador |
| `OUTBOX_RETRY_BASE`, `OUTBOX_RETRY_MAX` | `1s`, `5m` | backoff de publicação |
| `PENDING_POLL_INTERVAL`, `PENDING_BATCH_SIZE` | `500ms`, `50` | worker de referências |
| `PENDING_REFERENCE_TTL`, `PENDING_REFERENCE_MAX_ATTEMPTS` | `5m`, `20` | limite de espera por referência |
| `PENDING_BACKOFF_BASE`, `PENDING_BACKOFF_MAX` | `1s`, `30s` | backoff exponencial das pendências |
| `START_TIMEOUT`, `SHUTDOWN_TIMEOUT` | `60s`, `25s` | prazos do ciclo de vida Fx |

A configuração é validada na inicialização (valores obrigatórios, URLs, limites e coerência entre prazos); configuração inválida encerra o processo com código 2.

## 5. Filas SQS

Provisionadas automaticamente pelo LocalStack ao iniciar (`deploy/localstack/init-aws.sh`, idempotente):

| Fila | Tipo | Configuração |
| --- | --- | --- |
| `wager-transactions.fifo` | entrada | `VisibilityTimeout=30`, `RedrivePolicy` → DLQ com `maxReceiveCount=5`, deduplicação explícita |
| `wager-transactions-dlq.fifo` | DLQ de entrada | recebe mensagens inválidas (enviadas pelo consumidor) e tentativas esgotadas (redrive) |
| `wallet-events.fifo` | saída (outbox) | eventos de integração; redrive para `wallet-events-dlq.fifo` |

Para reprovisionar manualmente: `docker compose exec localstack /etc/localstack/init/ready.d/init-aws.sh`.

Exemplo de envio (produtor usa `MessageGroupId = walletId` e `MessageDeduplicationId = messageId`):

```sh
aws --endpoint-url http://localhost:4566 --region us-east-1 sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id "$WALLET_ID" --message-deduplication-id msg-123 \
  --message-body '{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z",
    "data":{"providerId":"provider-a","externalTransactionId":"transaction-123","idempotencyKey":"provider-a:transaction-123",
    "playerId":"'"$PLAYER_ID"'","walletId":"'"$WALLET_ID"'","roundId":"round-987","gameId":"fortune-chimp","kind":"BET",
    "money":{"amount":"25.00","currency":"BRL"}}}'
```

(`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` podem ter qualquer valor no LocalStack.)

## 6. Migrations

Arquivos versionados em [`migrations/`](migrations) (`000001_initial_schema.up.sql` / `.down.sql`), embutidos no binário `migrate` (golang-migrate, driver pgx v5).

```sh
# No Compose (executado automaticamente pelo serviço migrate):
docker compose run --rm migrate up
docker compose run --rm migrate down 1        # reverte a última
docker compose run --rm migrate version

# No host:
export MIGRATIONS_DATABASE_URL=postgres://wallet_owner:wallet_owner@localhost:5432/wallet?sslmode=disable
go run ./cmd/migrate up          # aplica pendentes
go run ./cmd/migrate down 1      # reverte N (padrão 1)
go run ./cmd/migrate down-all    # reverte tudo (apaga dados; apenas local/teste)
go run ./cmd/migrate goto 1      # vai até a versão 1
go run ./cmd/migrate force 1     # recuperação manual de estado "dirty"
```

## 7. Executando no host

```sh
docker compose up -d postgres keycloak localstack
set -a; . ./.env.example; set +a
go run ./cmd/migrate up
go run ./cmd/wallet-service          # API em http://localhost:8090
```

## 8. API

Todas as rotas de negócio exigem `Authorization: Bearer <access token>` do Keycloak. `X-Correlation-Id` é opcional (gerado se ausente) e devolvido na resposta.

| Rota | Quem pode | Descrição |
| --- | --- | --- |
| `POST /wallets` | `wallet-operator` | abre carteira |
| `GET /wallets/{walletId}` | `wallet-operator` | consulta saldo/versão |
| `GET /wallets/{walletId}/ledger?cursor=&limit=` | `wallet-operator` | ledger paginado (cursor opaco, ordem por versão) |
| `POST /wallets/{walletId}/reconciliation` | `wallet-operator` | reconstrói o saldo pelo ledger e compara |
| `POST /wagering/transactions` | `wagering-provider` (corpo com o próprio `providerId`) | envia operação; `Idempotency-Key` obrigatório |
| `GET /wagering/transactions/{transactionId}` | provedor dono ou `wallet-operator` | acompanha estado, `failureCode`, pendências |
| `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}` | o próprio provedor ou `wallet-operator` | consulta por ID externo |
| `GET /health/live`, `GET /health/ready`, `GET /metrics` | público | liveness, readiness (PostgreSQL + SQS) e métricas Prometheus |

### Exemplos

```sh
OP=$(curl -s -d grant_type=client_credentials -d client_id=wallet-service -d client_secret=wallet-service-secret \
  http://localhost:8080/realms/wagering/protocol/openid-connect/token | jq -r .access_token)

curl -s -X POST localhost:8081/wallets -H "Authorization: Bearer $OP" -H 'Content-Type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'
# 201 {"id":"...","playerId":"...","balance":{"amount":"1000.00","currency":"BRL"},"version":1,...}

curl -s -X POST localhost:8081/wagering/transactions -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
       "walletId":"<id>","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
# 201 {"transactionId":"...","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false,...}

# Reversões: acrescente "referenceExternalTransactionId":"transaction-123" (e kind REFUND/ROLLBACK)
curl -s -X POST localhost:8081/wallets/<id>/reconciliation -H "Authorization: Bearer $OP"
```

### Códigos de resposta

| Situação | HTTP | Corpo |
| --- | --- | --- |
| Operação processada | `201` | transação (`status: PROCESSED`, `balance`, `idempotentReplay: false`) |
| Replay de operação processada | `200` | mesma transação, saldo do processamento original, `idempotentReplay: true` |
| Aguardando referência (`PENDING_REFERENCE`) | `202` | transação com `nextAttemptAt` e `expiresAt` |
| Rejeição de negócio (definitiva) | `422` | transação com `status: REJECTED` e `failureCode` (replays devolvem o mesmo) |
| Entrada inválida (corrigível, nada persistido) | `400` | `{"error":{"code":"INVALID_MONEY","message":"...","retryable":false}}` |
| Content-Type incorreto / corpo grande | `415` / `413` | `error` |
| Token ausente, inválido ou expirado | `401` | `error.code = UNAUTHENTICATED` + `WWW-Authenticate` |
| Papel insuficiente ou `providerId` de outro provedor | `403` | `error.code = FORBIDDEN` / `PROVIDER_MISMATCH` |
| Carteira inexistente / recurso inexistente ou de outro provedor | `404` | `WALLET_NOT_FOUND` / `NOT_FOUND` |
| Chave reutilizada com outro conteúdo | `409` | `IDEMPOTENCY_KEY_CONFLICT` |
| Mesma operação com outra chave | `409` | `EXTERNAL_TRANSACTION_CONFLICT` |
| Carteira duplicada (jogador + moeda) | `409` | `WALLET_ALREADY_EXISTS` |
| Dependência indisponível (transitório) | `503` | `SERVICE_UNAVAILABLE`, `retryable: true`, `Retry-After: 1` |
| Falha permanente registrada (`FAILED`) | `500` | transação com `status: FAILED` e `failureCode: INTERNAL_PERMANENT_FAILURE` |

Códigos de entrada (400): `INVALID_REQUEST`, `INVALID_MONEY`, `INVALID_KIND`, `OPENING_NOT_ALLOWED`, `INVALID_AMOUNT_FOR_KIND`, `REFERENCE_REQUIRED`, `REFERENCE_NOT_ALLOWED`, `INVALID_CURSOR`. Códigos definitivos (`failureCode`): ver [ARCHITECTURE.md §8](ARCHITECTURE.md#8-códigos-de-falha).

## 9. Testes

```sh
go test ./...              # unitários (domínio, aplicação, autenticação)
go test -race ./...
go vet ./...
gofmt -l .                 # deve imprimir nada
```

### Integração (containers reais)

Os testes de integração usam a build tag `integration` e **PostgreSQL, Keycloak e LocalStack reais**. Eles:

- revertem e reaplicam as migrations no banco `wallet_test` (validando `down` e `up`);
- compilam o serviço com a build tag `faultinject` e iniciam **processos independentes** (até 3 simultâneos) com portas, pools e filas próprias;
- criam filas isoladas por teste (prefixo `it-...`) com redrive e as removem ao final.

```sh
docker compose up -d postgres keycloak localstack      # preparar dependências
go test -tags integration -race -count=1 -p 1 -v ./test/integration/...
IT_BINARY_RACE=1 go test -tags integration -race -count=1 -p 1 ./test/integration/...   # instâncias também com -race
```

Variáveis opcionais: `IT_PG_HOSTPORT` (`localhost:5432`), `IT_KEYCLOAK_URL` (`http://localhost:8080`), `IT_SQS_ENDPOINT` (`http://localhost:4566`).

### Simulação de falhas

O pacote `internal/platform/fault` é no-op no build padrão. Com `-tags faultinject`, as variáveis abaixo ativam pontos de falha (usadas pelos testes):

| Variável | Pontos | Efeito |
| --- | --- | --- |
| `FAULT_CRASH_POINTS` | `sqs.after_commit_before_delete`, `outbox.before_publish`, `outbox.after_publish_before_mark` | `SIGKILL` no próprio processo naquele ponto |
| `FAULT_ERROR_POINTS` | `sqs.before_process`, `outbox.publish` | erro transitório injetado |

Exemplo manual: `go build -tags faultinject -o /tmp/ws ./cmd/wallet-service && FAULT_CRASH_POINTS=sqs.after_commit_before_delete /tmp/ws`.

### Cenários cobertos

| Requisito | Teste |
| --- | --- |
| Money: parsing, escala, limites, overflow, moedas, JSON | `internal/domain/money` |
| Invariantes da carteira, ledger, máquina de estados, regras dos 5 tipos, política de zero, abertura interna | `internal/domain/wallet`, `internal/domain/wager` |
| Conflito de payload na mesma chave, hash HTTP = SQS, validação de entrada | `internal/app` |
| Validação de JWT (assinatura, expiração, emissor, audiência, tipo) | `internal/auth` |
| Migrations, constraints, imutabilidade do ledger, privilégios | `TestMigrationsAppliedAndReversible`, `TestDatabaseInvariants` |
| IdP real: credenciais ausentes/inválidas/expiradas/outro emissor/audiência | `TestAuthenticationWithRealIdP` |
| Isolamento entre provedores (consultas e replays), operações internas restritas, ausência de efeitos | `TestAuthorizationAndProviderIsolation` |
| Abertura atômica (OPENING + ledger + 2 eventos), zero, duplicada | `TestOpenWalletAtomicAndUnique` |
| 80.00 + 80.00 sobre 100.00 em instâncias diferentes, com reenvios | `TestTwoBetsOf80AcrossThreeInstances` |
| Mesma aposta 50× em paralelo (3 instâncias) + 10 mensagens SQS | `TestSameBetFiftyTimesAcrossHTTPAndSQS` |
| Carteiras distintas em paralelo; lock de uma não bloqueia outra | `TestDistinctWalletsProgressInParallel` |
| Reconciliação consistente e divergência (resposta, log, métrica) | `TestReconciliationReportsDivergence` |
| Inbox, reentrega, conflito de hash, DLQ, rejeição terminal, HTTP×SQS | `TestSQSInboxDeduplicationAndDLQ` |
| Retry transitório com backoff e DLQ por tentativas esgotadas | `TestSQSTransientFailuresRetryThenDLQ` |
| Queda após commit e antes da remoção da mensagem | `TestSQSCrashAfterCommitBeforeDelete` |
| SIGTERM com mensagens em andamento | `TestSQSGracefulShutdown` |
| Dois publicadores disputando a outbox | `TestOutboxCompetingPublishers` |
| Queda entre commit e publicação / entre publicação e confirmação (mesmo `eventId`) | `TestOutboxCrashBetweenCommitAndPublish`, `TestOutboxCrashBetweenPublishAndConfirm` |
| Retry com backoff da outbox | `TestOutboxRetryWithBackoff` |
| Reversão antes da referência: resolução após reinício / expiração | `TestPendingReferenceSurvivesRestartAndResolves`, `TestPendingReferenceExpires` |
| Reinício completo preserva idempotência e consistência | `TestFullRestartPreservesIdempotencyAndConsistency` |
| PostgreSQL / SQS indisponíveis | `TestPostgresUnavailable`, `TestSQSUnavailable` |
| Composição Fx, início/parada, término dos workers, rollback | `TestFxCompositionStartAndStop`, `TestFxStartFailsOnUnavailableDependency` |

Todos os testes verificam ao final o saldo armazenado contra créditos − débitos do ledger.

## 10. Observabilidade

- **Logs JSON** (`log/slog`) com `service`, `instanceId` e, quando disponíveis, `correlationId`, `messageId`, `transactionId`, `walletId`, `providerId`. Não são registrados tokens, segredos nem payloads financeiros.
- **Métricas** em `GET /metrics`: `wager_transactions_total{channel,kind,status,replay}`, `wager_processing_duration_seconds`, `wager_duplicates_total{reason}`, `wager_idempotency_conflicts_total`, `wallet_concurrency_conflicts_total{reason}`, `retries_total{component}`, `sqs_messages_total{outcome}`, `sqs_dlq_messages_total{reason}`, `outbox_pending_events`, `outbox_oldest_pending_age_seconds`, `outbox_publish_delay_seconds`, `outbox_publish_failures_total`, `wallet_reconciliation_divergences_total`, `auth_failures_total{reason}`, `http_requests_total`, `http_request_duration_seconds`.
- **Health checks**: `/health/live` (processo) e `/health/ready` (PostgreSQL e SQS; `503` durante o encerramento).

## 11. Estrutura

```
cmd/wallet-service      binário principal (Fx)
cmd/migrate             aplicação/reversão de migrations
internal/domain         Money, Wallet, LedgerEntry, WagerTransaction, eventos (sem Fx/HTTP/SQS/banco)
internal/app            casos de uso e portas (UnitOfWork, repositórios, métricas)
internal/adapters       postgres (pgx), httpapi (net/http), sqs (consumidor e publicador da outbox)
internal/auth           validação OIDC e modelo de permissões
internal/workers        worker de referências pendentes
internal/bootstrap      composição Fx (módulos)
internal/platform       config, logs, métricas, health, worker runner, injeção de falhas
migrations              SQL versionado
deploy                  provisionamento de PostgreSQL, Keycloak e LocalStack
test/integration        testes de integração (build tag integration)
```
