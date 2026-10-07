#!/usr/bin/env bash
# Demonstração dos fluxos autenticados contra uma instância em execução.
#
#   ./scripts/demo.sh                      # usa http://localhost:8081 (docker compose)
#   BASE_URL=http://localhost:8090 ./scripts/demo.sh
#
# Requer curl e jq.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8081}"
KEYCLOAK_URL="${KEYCLOAK_URL:-http://localhost:8080}"
TOKEN_URL="$KEYCLOAK_URL/realms/wagering/protocol/openid-connect/token"

token() {
  # $1 = client_id, $2 = client_secret (identidades de teste provisionadas no realm)
  curl -fsS -d grant_type=client_credentials -d "client_id=$1" -d "client_secret=$2" "$TOKEN_URL" | jq -r .access_token
}

OPERATOR=$(token wallet-service wallet-service-secret)
PROVIDER_A=$(token provider-a provider-a-secret)
PROVIDER_B=$(token provider-b provider-b-secret)

PLAYER_ID=$(uuidgen | tr 'A-Z' 'a-z')
SUFFIX=$(date +%s)

echo "== 1. Abertura de carteira (serviço interno)"
WALLET=$(curl -fsS -X POST "$BASE_URL/wallets" \
  -H "Authorization: Bearer $OPERATOR" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER_ID\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}")
echo "$WALLET" | jq .
WALLET_ID=$(echo "$WALLET" | jq -r .id)

bet_body() {
  echo "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$1\",\"playerId\":\"$PLAYER_ID\",\"walletId\":\"$WALLET_ID\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"$2\",\"money\":{\"amount\":\"$3\",\"currency\":\"BRL\"}$4}"
}

echo "== 2. BET de 25.00 (provider-a)"
curl -sS -X POST "$BASE_URL/wagering/transactions" -w '\nHTTP %{http_code}\n' \
  -H "Authorization: Bearer $PROVIDER_A" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: provider-a:bet-$SUFFIX" -d "$(bet_body "bet-$SUFFIX" BET 25.00 '')"

echo "== 3. Replay da mesma aposta (idempotentReplay=true, HTTP 200)"
curl -sS -X POST "$BASE_URL/wagering/transactions" -w '\nHTTP %{http_code}\n' \
  -H "Authorization: Bearer $PROVIDER_A" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: provider-a:bet-$SUFFIX" -d "$(bet_body "bet-$SUFFIX" BET 25.00 '')"

echo "== 4. Mesma chave com conteúdo diferente (409)"
curl -sS -X POST "$BASE_URL/wagering/transactions" -w '\nHTTP %{http_code}\n' \
  -H "Authorization: Bearer $PROVIDER_A" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: provider-a:bet-$SUFFIX" -d "$(bet_body "bet-$SUFFIX" BET 26.00 '')"

echo "== 5. ROLLBACK antes da referência (202 PENDING_REFERENCE)"
curl -sS -X POST "$BASE_URL/wagering/transactions" -w '\nHTTP %{http_code}\n' \
  -H "Authorization: Bearer $PROVIDER_A" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: provider-a:rb-$SUFFIX" \
  -d "$(bet_body "rb-$SUFFIX" ROLLBACK 10.00 ",\"referenceExternalTransactionId\":\"late-$SUFFIX\"")"

echo "== 6. A aposta referenciada chega depois"
curl -sS -X POST "$BASE_URL/wagering/transactions" -w '\nHTTP %{http_code}\n' \
  -H "Authorization: Bearer $PROVIDER_A" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: provider-a:late-$SUFFIX" -d "$(bet_body "late-$SUFFIX" BET 10.00 '')"
sleep 2
echo "== 7. Consulta do ROLLBACK (resolvido pelo worker)"
curl -sS "$BASE_URL/providers/provider-a/wagering/transactions/rb-$SUFFIX" -H "Authorization: Bearer $PROVIDER_A" | jq '{status, balance, referenceTransactionId}'

echo "== 8. provider-b não enxerga transações do provider-a (403/404)"
curl -sS -o /dev/null -w 'HTTP %{http_code}\n' "$BASE_URL/providers/provider-a/wagering/transactions/bet-$SUFFIX" -H "Authorization: Bearer $PROVIDER_B"
echo "== 9. provider não acessa operações de carteira (403)"
curl -sS -o /dev/null -w 'HTTP %{http_code}\n' "$BASE_URL/wallets/$WALLET_ID" -H "Authorization: Bearer $PROVIDER_A"
echo "== 10. sem token (401)"
curl -sS -o /dev/null -w 'HTTP %{http_code}\n' "$BASE_URL/wallets/$WALLET_ID"

echo "== 11. Ledger e reconciliação"
curl -fsS "$BASE_URL/wallets/$WALLET_ID/ledger?limit=50" -H "Authorization: Bearer $OPERATOR" | jq '.entries | map({direction, money: .money.amount, balanceAfter: .balanceAfter.amount, walletVersion})'
curl -fsS -X POST "$BASE_URL/wallets/$WALLET_ID/reconciliation" -H "Authorization: Bearer $OPERATOR" | jq .
