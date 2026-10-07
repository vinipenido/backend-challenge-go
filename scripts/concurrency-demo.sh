#!/usr/bin/env bash
# Disputa obrigatória contra as três instâncias do docker compose:
# carteira com 100.00 BRL recebe, ao mesmo tempo, duas apostas de 80.00 em
# instâncias diferentes (8081 e 8082); depois ambas são reenviadas pela 8083.
# Requer curl, jq e uuidgen.
set -euo pipefail

KEYCLOAK_URL="${KEYCLOAK_URL:-http://localhost:8080}"
TOKEN_URL="$KEYCLOAK_URL/realms/wagering/protocol/openid-connect/token"
token() { curl -fsS -d grant_type=client_credentials -d "client_id=$1" -d "client_secret=$2" "$TOKEN_URL" | jq -r .access_token; }

OPERATOR=$(token wallet-service wallet-service-secret)
PROVIDER=$(token provider-a provider-a-secret)
PLAYER=$(uuidgen | tr 'A-Z' 'a-z')
WALLET=$(curl -fsS -X POST http://localhost:8081/wallets -H "Authorization: Bearer $OPERATOR" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}" | jq -r .id)
echo "carteira $WALLET com 100.00 BRL"

bet() { # $1 porta, $2 id externo
  curl -sS -X POST "http://localhost:$1/wagering/transactions" -H "Authorization: Bearer $PROVIDER" \
    -H 'Content-Type: application/json' -H "Idempotency-Key: provider-a:$2" \
    -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$2\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"r1\",\"gameId\":\"g1\",\"kind\":\"BET\",\"money\":{\"amount\":\"80.00\",\"currency\":\"BRL\"}}" \
    | jq -c '{status, failureCode, balance: .balance.amount, idempotentReplay}'
}
A="race-a-$(date +%s%N)"; B="race-b-$(date +%s%N)"
bet 8081 "$A" & bet 8082 "$B" & wait
echo "reenvios pela instância 3:"
bet 8083 "$A"; bet 8083 "$B"
echo "saldo final e reconciliação:"
curl -fsS "http://localhost:8083/wallets/$WALLET" -H "Authorization: Bearer $OPERATOR" | jq -c '{balance: .balance.amount, version}'
curl -fsS -X POST "http://localhost:8081/wallets/$WALLET/reconciliation" -H "Authorization: Bearer $OPERATOR" | jq -c '{consistent, checkedEntries, difference: .difference.amount}'
