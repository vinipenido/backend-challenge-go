#!/bin/bash
# Provisionamento das filas SQS e das políticas de acesso no LocalStack.
# Executado automaticamente pelo LocalStack quando o serviço fica pronto
# (diretório /etc/localstack/init/ready.d). O script é idempotente.
set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ACCOUNT="000000000000"
MAX_RECEIVE_COUNT="${WAGER_QUEUE_MAX_RECEIVE_COUNT:-5}"
VISIBILITY_TIMEOUT="${WAGER_QUEUE_VISIBILITY_TIMEOUT:-30}"

create_fifo() {
  # $1 = nome da fila, $2 = atributos extras em JSON (opcional)
  local name="$1"
  local extra="${2:-}"
  local attrs="{\"FifoQueue\":\"true\",\"ContentBasedDeduplication\":\"false\",\"MessageRetentionPeriod\":\"1209600\""
  if [ -n "$extra" ]; then
    attrs="${attrs},${extra}"
  fi
  attrs="${attrs}}"
  awslocal sqs create-queue --region "$REGION" --queue-name "$name" --attributes "$attrs" >/dev/null
  echo "fila criada: $name"
}

arn_of() {
  echo "arn:aws:sqs:${REGION}:${ACCOUNT}:$1"
}

# Entrada: operações financeiras enviadas pelos provedores.
create_fifo "wager-transactions-dlq.fifo"
REDRIVE="{\\\"deadLetterTargetArn\\\":\\\"$(arn_of wager-transactions-dlq.fifo)\\\",\\\"maxReceiveCount\\\":\\\"${MAX_RECEIVE_COUNT}\\\"}"
create_fifo "wager-transactions.fifo" "\"VisibilityTimeout\":\"${VISIBILITY_TIMEOUT}\",\"RedrivePolicy\":\"${REDRIVE}\""

# Saída: eventos de integração publicados pela outbox.
create_fifo "wallet-events-dlq.fifo"
EVENTS_REDRIVE="{\\\"deadLetterTargetArn\\\":\\\"$(arn_of wallet-events-dlq.fifo)\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}"
create_fifo "wallet-events.fifo" "\"VisibilityTimeout\":\"30\",\"RedrivePolicy\":\"${EVENTS_REDRIVE}\""

# Políticas de acesso ao broker. Cada papel usa credenciais próprias:
#   - provedores: apenas sqs:SendMessage na fila de entrada;
#   - serviço de carteiras: consumo da entrada, envio para a DLQ e publicação de eventos;
#   - consumidores de eventos: leitura da fila de eventos.
# Observação: o LocalStack Community registra estas políticas, mas só as aplica
# com ENFORCE_IAM (recurso pago). Em AWS real elas são efetivas. Ver ARCHITECTURE.md.
cat > /tmp/policy-provider.json <<JSON
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["sqs:SendMessage","sqs:GetQueueUrl"],"Resource":"$(arn_of wager-transactions.fifo)"}]}
JSON
cat > /tmp/policy-wallet-service.json <<JSON
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Action":["sqs:ReceiveMessage","sqs:DeleteMessage","sqs:ChangeMessageVisibility","sqs:GetQueueAttributes","sqs:GetQueueUrl"],"Resource":"$(arn_of wager-transactions.fifo)"},
 {"Effect":"Allow","Action":["sqs:SendMessage","sqs:GetQueueUrl"],"Resource":["$(arn_of wager-transactions-dlq.fifo)","$(arn_of wallet-events.fifo)"]}
]}
JSON
cat > /tmp/policy-events-consumer.json <<JSON
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["sqs:ReceiveMessage","sqs:DeleteMessage","sqs:ChangeMessageVisibility","sqs:GetQueueUrl"],"Resource":"$(arn_of wallet-events.fifo)"}]}
JSON

for principal in provider-a provider-b wallet-service events-consumer; do
  awslocal iam create-user --user-name "$principal" >/dev/null 2>&1 || true
done
awslocal iam put-user-policy --user-name provider-a --policy-name send-wager-transactions --policy-document file:///tmp/policy-provider.json
awslocal iam put-user-policy --user-name provider-b --policy-name send-wager-transactions --policy-document file:///tmp/policy-provider.json
awslocal iam put-user-policy --user-name wallet-service --policy-name consume-and-publish --policy-document file:///tmp/policy-wallet-service.json
awslocal iam put-user-policy --user-name events-consumer --policy-name consume-wallet-events --policy-document file:///tmp/policy-events-consumer.json

echo "provisionamento SQS concluído"
