# Atalhos para os comandos documentados no README.
.PHONY: up down deps migrate-up migrate-down build fmt vet test test-race test-integration demo concurrency-demo

up: ## Sobe todo o ambiente (dependências, migrations e 3 instâncias)
	docker compose up --build

down: ## Derruba o ambiente e remove volumes
	docker compose down -v

deps: ## Sobe apenas as dependências (para testes de integração e execução no host)
	docker compose up -d postgres keycloak localstack

migrate-up: ## Aplica as migrations no banco local (host)
	MIGRATIONS_DATABASE_URL=postgres://wallet_owner:wallet_owner@localhost:5432/wallet?sslmode=disable go run ./cmd/migrate up

migrate-down: ## Reverte a última migration no banco local (host)
	MIGRATIONS_DATABASE_URL=postgres://wallet_owner:wallet_owner@localhost:5432/wallet?sslmode=disable go run ./cmd/migrate down 1

build:
	go build ./...

fmt:
	gofmt -l -w .

vet:
	go vet ./...
	go vet -tags integration ./test/integration/

test: ## Testes unitários
	go test ./...

test-race:
	go test -race ./...

test-integration: ## Requer `make deps`
	go test -tags integration -race -count=1 -p 1 -v ./test/integration/...

demo:
	./scripts/demo.sh

concurrency-demo:
	./scripts/concurrency-demo.sh
