# Arquitetura e decisões técnicas

Este documento registra as decisões sobre dinheiro, transações, idempotência, locks, referências pendentes, reversões, inbox/outbox, autenticação, autorização, uso do Fx e shutdown, além de limitações, interpretações adotadas e trabalho não concluído.

## 1. Visão geral

Um único binário (`cmd/wallet-service`) contém quatro componentes, todos compostos com Uber Fx e habilitáveis por configuração:

| Componente | Entrada | Saída |
| --- | --- | --- |
| API HTTP (`net/http`, padrões de rota do Go 1.22+) | provedores e serviço interno | respostas síncronas |
| Consumidor SQS | `wager-transactions.fifo` | mesma lógica do HTTP; DLQ |
| Worker de referências pendentes | `wager_transactions` (`PENDING_REFERENCE`) | conclusão/rejeição |
| Publicador da outbox | `outbox_events` | `wallet-events.fifo` |

Qualquer número de instâncias pode rodar ao mesmo tempo; **não existe estado em memória necessário à correção**: idempotência, pendências, leases e eventos ficam no PostgreSQL.

Camadas e dependências (setas = "depende de"):

```
cmd ─► bootstrap (Fx) ─► adapters (httpapi, sqs, postgres), auth, workers ─► app (casos de uso + portas) ─► domain
```

`internal/domain` não importa Fx, HTTP, SQS nem bibliotecas de persistência (apenas `uuid` e a biblioteca padrão).

## 2. Dinheiro

- **Representação**: `money.Money{minor int64, currency Currency}` — inteiro de 64 bits em unidades mínimas (centavos) com escala fixa de 2 casas e moeda ISO 4217. Limites: `-92233720368547758.08` a `92233720368547758.07`.
- **Moedas**: apenas códigos ISO 4217 com exatamente 2 casas decimais (BRL, USD, EUR, GBP, ARS, MXN, COP, PEN, UYU, CAD, AUD, CHF). Moedas com outra escala são rejeitadas, nunca arredondadas.
- **Parsing estrito** (`-?(0|[1-9][0-9]*)\.[0-9]{2}`): rejeita vazio, espaços, `+`, `NaN`, `Infinity`, notação científica, vírgula, separador de milhar, zeros à esquerda, escala ≠ 2 e `-0.00`. Acúmulo em `uint64` com checagem de overflow a cada dígito (inclusive o limite assimétrico de `MinInt64`).
- **Entradas externas**: `ParseNonNegative` (negativos recusados). JSON: `amount` precisa ser **string**; número JSON é recusado já na decodificação (`INVALID_MONEY`), então nenhum valor passa por `float64`. Não há normalização de formas equivalentes: `"25"`, `"25.0"` e `"brl"` são inválidos; com isso o hash de idempotência é calculado sobre a forma canônica recebida.
- **Aritmética**: `Add`, `Sub`, `Neg`, `Cmp` exigem moedas iguais (`ErrCurrencyMismatch`) e detectam overflow (`ErrOverflow`). Valores não inicializados (`Money{}`) são rejeitados (`ErrUninitialized`). Negativos são permitidos em diferenças internas; a não negatividade do saldo é regra da carteira (e do banco).
- **Serialização**: sempre `{"amount":"25.00","currency":"BRL"}`.
- **Persistência**: `BIGINT` (unidades mínimas) + `CHAR(3)`. Somatórios no banco (`SUM(...)::BIGINT`) falham em overflow em vez de perder precisão.

## 3. Banco de dados e transações

- **Biblioteca**: `pgx/v5` (`pgxpool`) com SQL explícito, sem ORM. Todo lock, constraint e transação é visível no código (`internal/adapters/postgres`) e no schema (`migrations/`).
- **Delimitação da transação**: a porta `app.UnitOfWork` expõe `Do(ctx, fn)` (READ COMMITTED) e `ReadSnapshot(ctx, fn)` (REPEATABLE READ READ ONLY). Dentro de `fn`, todos os repositórios (`Wallets`, `Transactions`, `Ledger`, `Outbox`, `Inbox`) compartilham a mesma `pgx.Tx`; o `COMMIT` só ocorre se `fn` devolver `nil`. Assim, estado da operação, saldo, ledger, inbox e eventos são confirmados atomicamente.
- **Papéis**: as migrations rodam como `wallet_owner`; a aplicação usa `wallet_app`, que **não tem** `UPDATE`, `DELETE` nem `TRUNCATE` no ledger e não pode excluir transações.
- **Proteções por sessão**: `lock_timeout` (5 s), `statement_timeout` (15 s) e `idle_in_transaction_session_timeout` (30 s).

### Invariantes impostas pelo schema

| Invariante | Mecanismo |
| --- | --- |
| Saldo ≥ 0 | `CHECK (balance_minor >= 0)` |
| Uma carteira por `(playerId, currency)` | `UNIQUE` |
| Mudança de saldo ⇒ versão +1; versão só muda com saldo; identidade imutável | trigger `wallets_guard_update` |
| Todo saldo tem o lançamento correspondente na mesma transação | constraint trigger **adiada para o COMMIT** `wallets_require_ledger` (versão e saldo final devem existir no ledger) |
| `balanceAfter = balanceBefore ± amount`; valores positivos; saldos ≥ 0 | `CHECK` no ledger |
| Encadeamento do ledger (`balance_before` = último `balance_after`; versão crescente; moeda da carteira) | trigger `ledger_chain_check` |
| Ledger append-only | triggers `BEFORE UPDATE/DELETE/TRUNCATE` + ausência de privilégios |
| Unicidade `(walletId, transactionId)` e `(walletId, walletVersion)` | `UNIQUE` |
| Idempotência por `(providerId, idempotencyKey)` e operação por `(providerId, externalTransactionId)` | índices únicos parciais |
| Um único crédito inicial por carteira | índice único parcial `kind = 'OPENING'` |
| No máximo uma reversão bem-sucedida por referência | índice único parcial em `reference_transaction_id` (`REFUND`/`ROLLBACK` `PROCESSED`) |
| Origem interna sem metadados externos; externa com todos | `CHECK wager_tx_origin_ck` |
| Política de valores por tipo (`LOSS` = 0, demais > 0) | `CHECK wager_tx_amount_ck` |
| Estados terminais imutáveis; campos de negócio imutáveis | trigger `wager_tx_guard_update` |
| `failure_code` ⇔ `REJECTED`/`FAILED`; `PROCESSED` ⇒ saldo resultante | `CHECK` |
| Snapshot da outbox imutável | trigger `outbox_guard_update` (só campos de entrega mudam) |
| Inbox única por `(consumerName, messageId)` | `PRIMARY KEY` |

Essas garantias valem mesmo sem os locks da aplicação e sem a deduplicação do SQS FIFO (`TestDatabaseInvariants`).

## 4. Concorrência

**Estratégia**: combinação de **lock pessimista por carteira** com **atualização condicionada à versão** e as constraints acima.

1. Cada operação abre uma transação e executa `SELECT ... FROM wallets WHERE id = $1 FOR UPDATE`. Somente operações **da mesma carteira** se serializam; carteiras diferentes avançam em paralelo (não há lock global, advisory lock nem mutex em memória). `TestDistinctWalletsProgressInParallel` segura o lock de uma carteira e mostra outra sendo processada.
2. O saldo é gravado com `UPDATE wallets ... WHERE id = $1 AND version = $expected`; se nenhuma linha for afetada, é conflito (defesa em profundidade: nenhum lost update, mesmo que algum caminho esqueça o lock).
3. Ordem de locks fixa (carteira → transação) em todos os caminhos, inclusive no worker de pendências, evitando deadlocks.
4. Conflitos detectados pelo banco (`40001`, `40P01`, `55P03`, violação de unicidade em corrida, versão divergente) viram `ConcurrencyConflictError`; o caso de uso repete **a transação inteira** até 5 vezes com backoff e jitter. Na repetição, a idempotência devolve o resultado já confirmado (replay) ou o conflito correto.
5. A checagem de idempotência acontece antes do lock (replays não disputam a carteira) e **de novo sob o lock**. As duas condições (mesma chave **ou** mesmo ID externo) são avaliadas **numa única consulta**: em READ COMMITTED, duas consultas separadas poderiam ver estados diferentes (falha encontrada pelo teste de 50 envios simultâneos e corrigida).

Demonstração com três processos independentes: `TestTwoBetsOf80AcrossThreeInstances`, `TestSameBetFiftyTimesAcrossHTTPAndSQS`, `TestDistinctWalletsProgressInParallel` e `./scripts/concurrency-demo.sh` (Compose).

## 5. Idempotência

- **Persistente**: a própria linha de `wager_transactions` é o registro de idempotência (chave, hash, resultado, saldo observado). Sobrevive ao reinício de todos os processos (`TestFullRestartPreservesIdempotencyAndConsistency`).
- **Escopo**: por provedor. `(providerId, idempotencyKey)` e `(providerId, externalTransactionId)` são únicos. O servidor usa a `Idempotency-Key` exatamente como recebida (nunca a substitui por uma chave calculada); no SQS a chave é `data.idempotencyKey`.
- **Hash**: SHA-256 (hex) do JSON canônico dos campos de negócio — objeto com chaves em ordem lexicográfica, sem espaços, sem escape de HTML, contendo `externalTransactionId`, `gameId`, `kind`, `money{amount,currency}`, `playerId`, `providerId`, `referenceExternalTransactionId` (só quando presente), `roundId`, `walletId`. Normalização: UUIDs aceitos apenas na forma canônica de 36 caracteres e convertidos para minúsculas; `amount` já é canônico pela gramática estrita. Ficam de fora a chave e os metadados de transporte (headers, `messageId`, `type`, `occurredAt`, `correlationId`). Por isso HTTP e SQS produzem o mesmo hash (`TestCanonicalHashEquivalence`).
- **Comportamento**:
  - mesma chave e mesmo hash → resultado persistido, `idempotentReplay: true`, **com o saldo observado no processamento original** (coluna `result_balance_minor`);
  - mesma chave, hash diferente → `409 IDEMPOTENCY_KEY_CONFLICT` (SQS: DLQ `idempotency_conflict`);
  - mesma operação `(providerId, externalTransactionId)` com outra chave → `409 EXTERNAL_TRANSACTION_CONFLICT`.
- **Inbox (SQS)**: além da idempotência financeira, `(consumerName, messageId)` deduplica reentregas; o hash da mensagem (SHA-256 do corpo canonicalizado, números preservados como texto) é verificado e conteúdo diferente com o mesmo `messageId` vai para a DLQ (`message_hash_conflict`).

## 6. Máquina de estados

```
PENDING ──► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE
PENDING_REFERENCE ──► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE (reagendamento)
PROCESSED, REJECTED, FAILED: terminais
```

- As transições são métodos do agregado (`MarkProcessed`, `MarkRejected`, `MarkFailed`, `MarkPendingReference`) que validam origem, estado e códigos; transições inválidas devolvem `ErrInvalidTransition`. O banco reforça a imutabilidade dos terminais.
- **Criação vs. reidratação**: `NewExternal`/`NewOpening` criam em `PENDING`; `Rehydrate(Snapshot)` valida a coerência do estado persistido e **não** reaplica movimentações nem emite eventos (o mesmo vale para `wallet.Rehydrate`).
- **Aceite síncrono**: operações são decididas na mesma transação em que são registradas; **nenhum `PENDING` é confirmado** (não há commit intermediário de aceite). Por isso não existe o cenário "interromper após confirmar `PENDING`". Ainda assim, o worker de pendências também retoma qualquer `PENDING` confirmado (`COALESCE(next_attempt_at, updated_at)`), caso um fluxo assíncrono seja introduzido.
- **Falhas transitórias vs. permanentes**:
  - *transitórias*: conexão recusada/perdida, timeouts, cancelamento, `08xxx`, `53xxx`, `57P0x`, `57014`, erros do SQS → nada é persistido; HTTP `503` (`retryable: true`), SQS reentrega com backoff, workers tentam de novo;
  - *conflitos de concorrência*: repetidos internamente (seção 4);
  - *permanentes*: qualquer outro erro inesperado (constraint ou trigger violados, dado corrompido) → a operação é registrada em transação própria como `FAILED` (`INTERNAL_PERMANENT_FAILURE`), sem efeito financeiro, para auditoria; replays devolvem esse resultado. No SQS, a mensagem vai para a DLQ (`permanent_failure`).

## 7. Operações, referências e reversões

| Tipo | Movimento | Regras |
| --- | --- | --- |
| `BET` | débito | valor > 0; sem referência; saldo insuficiente → `INSUFFICIENT_FUNDS` |
| `WIN` | crédito | valor > 0; referência opcional (se informada: `BET` da mesma rodada) |
| `LOSS` | nenhum | valor `0.00`; moeda da carteira; referência opcional (`BET`); sem ledger e sem mudança de versão; emite apenas `WagerTransactionProcessed` |
| `REFUND` | crédito | referência obrigatória a uma `BET` `PROCESSED`, valor integral |
| `ROLLBACK` | contrário ao original | referência obrigatória a `BET` (crédito), `WIN` (débito) ou `REFUND` (débito) `PROCESSED`, valor integral |

- A referência é resolvida por `(providerId, referenceExternalTransactionId)`; operação e referência precisam concordar em provedor, jogador, carteira, moeda e rodada (`REFERENCE_MISMATCH`). A referência interna resolvida é persistida em `reference_transaction_id`.
- **Combinações REFUND/ROLLBACK** (política adotada):
  - uma `BET` admite **no máximo uma reversão bem-sucedida, qualquer que seja o tipo** — `REFUND` e `ROLLBACK` da mesma aposta são mutuamente exclusivos (`REFERENCE_ALREADY_REVERSED`), o que impede devolver duas vezes o mesmo débito;
  - um `REFUND` pode ser desfeito por um `ROLLBACK` (débito de volta); depois disso a `BET` **não** pode ser reembolsada de novo;
  - `WIN` admite um único `ROLLBACK`; `LOSS`, `ROLLBACK` e `OPENING` não são reversíveis (`REFERENCE_KIND_NOT_ALLOWED`);
  - garantido pela regra de domínio sob o lock da carteira **e** pelo índice único parcial no banco.
- Reversão que precisaria debitar mais que o saldo → `REJECTED` com `REVERSAL_INSUFFICIENT_FUNDS` (código distinto de `INSUFFICIENT_FUNDS`), auditável com evento de rejeição.

### Referências ainda indisponíveis

- Referência inexistente → a operação é confirmada como `PENDING_REFERENCE` com `next_attempt_at` e `expires_at` (TTL), e emite `WagerTransactionPendingReference` uma única vez. No SQS, a mensagem de entrada é concluída nesse mesmo commit; o worker assume a continuidade.
- O worker (todas as instâncias) seleciona pendências vencidas, trava a carteira e então a pendência (`FOR UPDATE SKIP LOCKED`), reavalia e reagenda com **backoff exponencial** (`PENDING_BACKOFF_BASE` × 2ⁿ até `PENDING_BACKOFF_MAX`). O agendamento está no banco, então sobrevive a reinícios (`TestPendingReferenceSurvivesRestartAndResolves`).
- Quando a referência chega, a instância que a processou antecipa (`next_attempt_at = now()`) as pendências dependentes, após o commit (melhor esforço; o worker garante a retomada de qualquer forma).
- Esgotado o TTL ou `PENDING_REFERENCE_MAX_ATTEMPTS`: `REJECTED` com `REFERENCE_NOT_FOUND` (referência nunca chegou) ou `REFERENCE_STILL_PENDING` (existe, mas continua pendente), com `WagerTransactionRejected`.
- Referência existente e ainda pendente → continua aguardando. Referência que terminou sem sucesso (`REJECTED`/`FAILED`) → `REJECTED` com `REFERENCE_NOT_PROCESSED`.

## 8. Códigos de falha

**Entrada corrigível** (nada é persistido; HTTP 400/404, SQS → DLQ, pode ser reenviada corrigida): `INVALID_REQUEST`, `INVALID_MONEY`, `INVALID_KIND`, `OPENING_NOT_ALLOWED`, `INVALID_AMOUNT_FOR_KIND`, `REFERENCE_REQUIRED`, `REFERENCE_NOT_ALLOWED`, `INVALID_CURSOR`, `WALLET_NOT_FOUND`.

**Resultado definitivo** (persistido em `failure_code`; replays devolvem o mesmo):

| Código | Estado | Significado |
| --- | --- | --- |
| `INSUFFICIENT_FUNDS` | REJECTED | `BET` maior que o saldo |
| `REVERSAL_INSUFFICIENT_FUNDS` | REJECTED | reversão que debitaria além do saldo |
| `CURRENCY_MISMATCH` | REJECTED | moeda diferente da carteira |
| `PLAYER_WALLET_MISMATCH` | REJECTED | jogador não é o dono da carteira |
| `REFERENCE_NOT_FOUND` | REJECTED | referência não chegou até o TTL/tentativas |
| `REFERENCE_STILL_PENDING` | REJECTED | referência existe, mas não concluiu até o TTL |
| `REFERENCE_NOT_PROCESSED` | REJECTED | referência terminou `REJECTED`/`FAILED` |
| `REFERENCE_MISMATCH` | REJECTED | provedor, jogador, carteira, moeda ou rodada divergem |
| `REFERENCE_KIND_NOT_ALLOWED` | REJECTED | tipo referenciado não admite a operação |
| `REFERENCE_AMOUNT_MISMATCH` | REJECTED | reversão parcial ou com valor diferente |
| `REFERENCE_ALREADY_REVERSED` | REJECTED | referência já revertida com sucesso |
| `INTERNAL_PERMANENT_FAILURE` | FAILED | falha permanente de infraestrutura (auditoria) |

## 9. SQS: consumidor

- **Envelope** estrito (campos desconhecidos rejeitados): `messageId` (identidade durável), `type = WagerTransactionRequested`, `occurredAt` (RFC 3339), `correlationId` opcional e `data` com os mesmos campos do corpo HTTP + `idempotencyKey`. O caso de uso é **o mesmo** do HTTP (`WageringService.Process`), com o registro na inbox dentro da mesma transação SQL das alterações de domínio, do ledger e dos eventos.
- **Produtores**: `MessageGroupId = walletId` (ordem por carteira, paralelismo entre carteiras) e `MessageDeduplicationId = messageId` (deduplicação do SQS por 5 minutos — otimização; a garantia real é a inbox + idempotência).
- **Processamento**: lotes de até 10 mensagens; sequencial dentro de cada `MessageGroupId`, paralelo entre grupos; prazo próprio por mensagem (`SQS_PROCESSING_TIMEOUT` < `VisibilityTimeout`).
- **Remoção**: `DeleteMessage` só depois do `COMMIT`. Se o processo cair entre os dois (`TestSQSCrashAfterCommitBeforeDelete`), a reentrega é reconhecida pela inbox e removida sem novo efeito.
- **Rejeições de negócio** confirmadas (`REJECTED`) são terminais: a mensagem é removida.
- **Falhas transitórias**: a mensagem não é removida; o consumidor ajusta a visibilidade para `SQS_RETRY_BASE × 2^(receiveCount-1)` (até `SQS_RETRY_MAX`). Após `maxReceiveCount = 5` (redrive), o SQS move para `wager-transactions-dlq.fifo`.
- **Erros permanentes / mensagens inválidas** (JSON inválido, tipo desconhecido, payload inválido, provedor não confiável, conflito de hash ou de idempotência, carteira inexistente, falha permanente): o consumidor envia imediatamente para a DLQ com o atributo `dlqReason` e então remove da entrada (deduplicação na DLQ pelo `MessageId` do SQS, caso caia entre envio e remoção).
- **Visibility timeout**: 30 s na fila; processamento limitado a 15 s.
- **Shutdown (`SIGTERM`)**: o long polling é cancelado (nenhuma busca nova); mensagens do lote ainda não iniciadas têm a visibilidade zerada (reentrega imediata para outra instância); as em andamento terminam dentro do prazo (`TestSQSGracefulShutdown`).
- **Concorrência HTTP × SQS**: ambos passam pelo mesmo lock de carteira e pelas mesmas chaves únicas (`TestSameBetFiftyTimesAcrossHTTPAndSQS`, `TestSQSInboxDeduplicationAndDLQ`).

## 10. Outbox transacional e eventos

- Eventos são gravados em `outbox_events` **na mesma transação** que os originou; nada é publicado antes do commit.
- **Publicador** (todas as instâncias): reivindica lotes com `UPDATE ... WHERE event_id IN (SELECT ... FOR UPDATE SKIP LOCKED)` gravando `locked_by` (instância + sufixo aleatório) e `locked_until` (lease); publica; marca `published_at`. Vários publicadores disputam sem bloqueio mútuo (`TestOutboxCompetingPublishers`).
- **Falha de publicação**: `attempts`, `last_error` e `next_attempt_at` com backoff exponencial (`OUTBOX_RETRY_BASE`…`OUTBOX_RETRY_MAX`); eventos nunca são descartados.
- **Trabalho abandonado**: se a instância cair com lease ativo, outra instância reivindica após `locked_until` (`TestOutboxCrashBetweenCommitAndPublish`). No encerramento gracioso os leases são liberados imediatamente.
- **Queda entre publicar e confirmar**: o evento é republicado com o **mesmo `eventId`** e exatamente o mesmo payload (o JSON é um snapshot gravado em coluna `JSON`, imutável por trigger) — `TestOutboxCrashBetweenPublishAndConfirm`.
- **Ordem por carteira**: dentro de um lote, uma falha bloqueia os eventos seguintes da mesma carteira.

### Destino e contrato de roteamento

- Fila `wallet-events.fifo` (DLQ `wallet-events-dlq.fifo`).
- `MessageGroupId = walletId`; `MessageDeduplicationId = eventId`; atributos `eventType`, `eventId`, `eventVersion`, `correlationId`.
- Entrega **at-least-once**: consumidores devem deduplicar por `eventId` e usar `walletVersion` (em `WalletBalanceChanged`) para ordenar/descartar eventos antigos de saldo.

### Envelope e tipos

```json
{
  "eventId": "uuid v7", "eventType": "WalletBalanceChanged", "aggregateType": "Wallet", "aggregateId": "...",
  "correlationId": "...", "causationId": "...", "occurredAt": "2026-09-08T12:00:00.000Z", "version": 1,
  "data": { "walletId": "...", "transactionId": "...", "direction": "DEBIT",
            "money": {"amount":"25.00","currency":"BRL"}, "balanceBefore": {...}, "balanceAfter": {...}, "walletVersion": 2 }
}
```

| Evento | Gatilho | `data` |
| --- | --- | --- |
| `WagerTransactionProcessed` | conclusão com sucesso (inclui `LOSS` e `OPENING`) | ids, origem, tipo, `money`, `balanceAfter`, referências, `processedAt` |
| `WagerTransactionRejected` | rejeição definitiva | ids, tipo, `money`, `failureCode`, `rejectedAt` |
| `WalletBalanceChanged` | mudança efetiva de saldo | `walletId`, `transactionId`, `direction`, `money`, `balanceBefore`, `balanceAfter`, `walletVersion` |
| `WagerTransactionPendingReference` | entrada em espera por referência | ids, `referenceExternalTransactionId`, `nextAttemptAt`, `expiresAt` |

Cada tipo tem struct e construtor próprios (`internal/domain/event`) que fixam `eventType` e `version`. Timestamps em UTC RFC 3339; valores monetários como strings decimais. `correlationId` vem do header `X-Correlation-Id` (HTTP) ou do envelope/`messageId` (SQS); `causationId` é o `messageId` de entrada (operações) ou o `transactionId` (saldo). Eventos de origem interna (`OPENING`) omitem os metadados externos inaplicáveis.

## 11. Autenticação

- **IdP**: Keycloak 26 (realm `wagering`), provisionado automaticamente por import. Escolhido por ser OIDC completo, open source, executável em container e com suporte nativo a `client_credentials`, papéis e mapeadores de claims — sem emissão própria de tokens nem cadastro de senhas no serviço.
- **Fluxo**: `client_credentials` (comunicação entre serviços). Cada provedor e o serviço interno são clientes confidenciais.
- **Validação** (`go-oidc/v3`): assinatura **RS256** com chaves do endpoint JWKS (cache e rotação por `kid`), `iss` exato, `aud` contendo `wallet-api` (mapeador de audiência no Keycloak), `exp` sem tolerância e `typ = Bearer` (recusa ID tokens). Qualquer falha → `401` sem tocar em dados. O JWKS é verificado na inicialização.
- `OIDC_ISSUER_URL` e `OIDC_JWKS_URL` são separados: o emissor é fixo (`http://keycloak:8080/...`), enquanto as chaves podem ser buscadas por outro endereço (por exemplo, `localhost` nos testes).

## 12. Autorização

- **Papéis** (realm roles): `wagering-provider` (provedores) e `wallet-operator` (serviço interno).
- **Identidade → provedor**: o `providerId` autorizado vem **exclusivamente** do claim `provider_id`, definido no IdP por cliente (mapeador fixo). O corpo `providerId` deve ser igual ao do token (`403 PROVIDER_MISMATCH`), então um provedor nunca cria, consulta ou faz replay em nome de outro.
- **Escopo das consultas**: idempotência e buscas são sempre por `(providerId, ...)`. `GET /wagering/transactions/{id}` de outro provedor responde `404` (não revela existência); `GET /providers/{outro}/...` responde `403`.
- **Operações de carteira** (abrir, consultar, ledger, reconciliação) exigem `wallet-operator`; provedores recebem `403`. O serviço interno não pode enviar operações de provedores.
- **Mensageria**: o acesso ao broker usa credenciais por papel e políticas IAM provisionadas (`deploy/localstack/init-aws.sh`): provedores só `sqs:SendMessage` na fila de entrada; o serviço só consome a entrada e publica na DLQ e na fila de eventos; consumidores de eventos só leem a fila de eventos. No consumidor, as validações de domínio são as mesmas do HTTP e o `providerId` precisa estar em `SQS_TRUSTED_PROVIDERS`.

## 13. Uber Fx: composição e ciclo de vida

- Módulos (`fx.Module`): `platform` (logs, métricas, relógio, IDs, health), `postgres`, `auth`, `app` (casos de uso), `sqs` (cliente, filas, consumidor, publicador), `workers`, `http`. Construtores com injeção por parâmetros (`fx.In`), `fx.Provide`, `fx.Annotate`/`fx.As` para portas e grupo `readiness` para health checks; `fx.Invoke` ativa os componentes.
- **Início** (ordem garantida por `fx.Invoke` e dependências): configuração validada antes do Fx → pool do PostgreSQL (ping) → JWKS do IdP → URLs das filas → consumidor, publicador e worker → servidor HTTP. Cada dependência é verificada com novas tentativas, reservando parte do prazo para que o Fx consiga **reverter** os componentes já iniciados caso uma dependência falhe (`TestFxStartFailsOnUnavailableDependency`).
- **Workers**: `worker.Runner` com contexto cancelável, prazo de parada e término observável (`Done()`/`Running()`), verificado em `TestFxCompositionStartAndStop`.
- **Shutdown** (`SIGTERM`/`SIGINT`, `SHUTDOWN_TIMEOUT`), em ordem inversa ao início:
  1. HTTP: readiness passa a `503`, `http.Server.Shutdown` recusa novas conexões e aguarda as requisições em andamento;
  2. workers: param de buscar trabalho; o consumidor conclui ou devolve mensagens; o publicador libera leases;
  3. dependências: o pool do PostgreSQL é fechado por último.

## 14. Observabilidade

Logs JSON com identificadores de rastreio, sem credenciais nem payloads; métricas Prometheus de resultados por status, duplicatas, conflitos de idempotência e de concorrência, retries, DLQ, atraso da outbox (idade do evento mais antigo e histograma de atraso), latência e divergências de reconciliação; health checks de liveness e readiness. Lista completa no README.

## 15. Reconciliação

`POST /wallets/{id}/reconciliation` executa em `REPEATABLE READ READ ONLY` (visão consistente): lê o saldo armazenado e soma créditos − débitos de todo o ledger (incluindo a abertura). `difference = armazenado − reconstruído`. Divergências aparecem na resposta (`consistent: false`), em log de erro e na métrica `wallet_reconciliation_divergences_total`; o saldo nunca é alterado (`TestReconciliationReportsDivergence` provoca a divergência desligando triggers como superusuário).

## 16. Limitações, interpretações e trabalho não concluído

- **IAM no LocalStack**: a edição Community registra, mas não aplica, as políticas IAM (o enforcement é pago), e o `SenderId` devolvido é sempre a conta, não o usuário. Por isso a separação de credenciais/políticas está provisionada e documentada (efetiva em AWS real), mas no ambiente local a proteção efetiva da fila de entrada é a validação de domínio + `SQS_TRUSTED_PROVIDERS`. Em produção recomenda-se uma fila por provedor ou mapear `SenderId` → `providerId`.
- **Ordem global de eventos**: com vários publicadores, eventos de uma mesma carteira podem ser publicados fora de ordem entre lotes distintos; o contrato pede deduplicação por `eventId` e ordenação por `walletVersion`.
- **Janela de deduplicação do SQS**: republicações dentro de 5 minutos são descartadas pelo próprio SQS FIFO (mesmo `MessageDeduplicationId`); depois disso chegam como duplicatas, tratadas pelos consumidores via `eventId`. O teste de republicação usa uma fila nova para observar a segunda entrega.
- **Interpretações**: `LOSS` e `WIN` aceitam referência opcional a uma `BET`; `BET` não aceita referência. Reverter uma `BET` que já gerou `WIN` é permitido (cada reversão é independente). Uma `BET` reembolsada e com o reembolso desfeito não pode ser reembolsada de novo. `WALLET_NOT_FOUND` é tratado como entrada corrigível (não persistido). Rejeições guardam o saldo observado no momento da decisão.
- **Aceite assíncrono** não foi implementado (todas as operações sem dependência são concluídas de forma síncrona); o worker já trataria `PENDING` confirmados.
- **Não implementados (opcionais)**: ledger de partidas dobradas, tracing com OpenTelemetry, dashboards e testes de carga. Também não há rotina de limpeza/arquivamento de outbox e inbox antigas nem ferramenta de redrive da DLQ (feito manualmente pelo console/CLI do SQS).
- O cursor de paginação é opaco (base64url) e vinculado à carteira, mas não é assinado; `/metrics` é público na mesma porta da API.
