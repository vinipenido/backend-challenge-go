-- Schema inicial: carteiras, transações, ledger, outbox e inbox.
-- Valores monetários: BIGINT em unidades mínimas (centavos) + CHAR(3) ISO 4217.
-- As invariantes financeiras são impostas aqui, independentemente da aplicação.

-- ---------------------------------------------------------------------------
-- Carteiras
-- ---------------------------------------------------------------------------
CREATE TABLE wallets (
    id             UUID        PRIMARY KEY,
    player_id      UUID        NOT NULL,
    currency       CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_minor  BIGINT      NOT NULL CHECK (balance_minor >= 0),
    version        BIGINT      NOT NULL CHECK (version >= 1),
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    CONSTRAINT wallets_player_currency_uk UNIQUE (player_id, currency)
);

-- ---------------------------------------------------------------------------
-- Transações (operações externas e abertura interna)
-- ---------------------------------------------------------------------------
CREATE TABLE wager_transactions (
    id                                UUID        PRIMARY KEY,
    origin                            TEXT        NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    kind                              TEXT        NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    status                            TEXT        NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    wallet_id                         UUID        NOT NULL REFERENCES wallets (id),
    player_id                         UUID        NOT NULL,
    amount_minor                      BIGINT      NOT NULL CHECK (amount_minor >= 0),
    currency                          CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    provider_id                       TEXT,
    external_transaction_id           TEXT,
    idempotency_key                   TEXT,
    payload_hash                      CHAR(64),
    round_id                          TEXT,
    game_id                           TEXT,
    reference_external_transaction_id TEXT,
    reference_transaction_id          UUID        REFERENCES wager_transactions (id),
    failure_code                      TEXT,
    result_balance_minor              BIGINT      CHECK (result_balance_minor >= 0),
    correlation_id                    TEXT,
    causation_id                      TEXT,
    attempts                          INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at                   TIMESTAMPTZ,
    expires_at                        TIMESTAMPTZ,
    created_at                        TIMESTAMPTZ NOT NULL,
    updated_at                        TIMESTAMPTZ NOT NULL,
    completed_at                      TIMESTAMPTZ,

    -- Origem interna (OPENING) não possui metadados externos; origem externa exige todos.
    CONSTRAINT wager_tx_origin_ck CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING'
            AND provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL
            AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    -- Política de valores: LOSS exige zero; os demais tipos exigem valor positivo.
    CONSTRAINT wager_tx_amount_ck CHECK (
        (kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)
    ),
    CONSTRAINT wager_tx_reference_ck CHECK (
        kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL
    ),
    CONSTRAINT wager_tx_failure_ck CHECK (
        (status IN ('REJECTED', 'FAILED')) = (failure_code IS NOT NULL)
    ),
    CONSTRAINT wager_tx_processed_balance_ck CHECK (
        status <> 'PROCESSED' OR result_balance_minor IS NOT NULL
    ),
    CONSTRAINT wager_tx_pending_schedule_ck CHECK (
        status <> 'PENDING_REFERENCE' OR (next_attempt_at IS NOT NULL AND expires_at IS NOT NULL)
    ),
    CONSTRAINT wager_tx_terminal_ck CHECK (
        (status IN ('PROCESSED', 'REJECTED', 'FAILED')) = (completed_at IS NOT NULL)
    ),
    CONSTRAINT wager_tx_opening_status_ck CHECK (
        kind <> 'OPENING' OR status IN ('PENDING', 'PROCESSED', 'FAILED')
    )
);

-- Idempotência persistente: chave por provedor e operação financeira por provedor.
CREATE UNIQUE INDEX wager_tx_provider_idempotency_uk
    ON wager_transactions (provider_id, idempotency_key) WHERE origin = 'EXTERNAL';
CREATE UNIQUE INDEX wager_tx_provider_external_uk
    ON wager_transactions (provider_id, external_transaction_id) WHERE origin = 'EXTERNAL';
-- No máximo um crédito inicial por carteira.
CREATE UNIQUE INDEX wager_tx_single_opening_uk
    ON wager_transactions (wallet_id) WHERE kind = 'OPENING';
-- Uma referência recebe no máximo uma reversão bem-sucedida (REFUND ou ROLLBACK).
CREATE UNIQUE INDEX wager_tx_single_reversal_uk
    ON wager_transactions (reference_transaction_id)
    WHERE kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED';
-- Fila durável de pendências (retomada por qualquer instância).
CREATE INDEX wager_tx_due_pending_idx
    ON wager_transactions (next_attempt_at) WHERE status IN ('PENDING', 'PENDING_REFERENCE');
CREATE INDEX wager_tx_pending_reference_idx
    ON wager_transactions (provider_id, reference_external_transaction_id) WHERE status = 'PENDING_REFERENCE';
CREATE INDEX wager_tx_wallet_idx ON wager_transactions (wallet_id, created_at);

-- ---------------------------------------------------------------------------
-- Ledger append-only
-- ---------------------------------------------------------------------------
CREATE TABLE wallet_ledger_entries (
    id                    UUID        PRIMARY KEY,
    wallet_id             UUID        NOT NULL REFERENCES wallets (id),
    transaction_id        UUID        NOT NULL REFERENCES wager_transactions (id),
    direction             TEXT        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_minor          BIGINT      NOT NULL CHECK (amount_minor > 0),
    currency              CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_before_minor  BIGINT      NOT NULL CHECK (balance_before_minor >= 0),
    balance_after_minor   BIGINT      NOT NULL CHECK (balance_after_minor >= 0),
    wallet_version        BIGINT      NOT NULL CHECK (wallet_version >= 1),
    created_at            TIMESTAMPTZ NOT NULL,
    CONSTRAINT ledger_wallet_transaction_uk UNIQUE (wallet_id, transaction_id),
    CONSTRAINT ledger_wallet_version_uk UNIQUE (wallet_id, wallet_version),
    CONSTRAINT ledger_balance_math_ck CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
        OR
        (direction = 'DEBIT' AND balance_after_minor = balance_before_minor - amount_minor)
    )
);

-- Imutabilidade do ledger: nenhuma alteração ou exclusão, nem mesmo pelo dono.
CREATE FUNCTION ledger_forbid_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries é append-only: % não permitido', TG_OP
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;

CREATE TRIGGER ledger_forbid_update_delete
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_forbid_mutation();

CREATE TRIGGER ledger_forbid_truncate
    BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_forbid_mutation();

-- Encadeamento: cada lançamento parte do saldo final do lançamento anterior da
-- mesma carteira, com moeda da carteira e versão maior que a anterior.
CREATE FUNCTION ledger_check_chain() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    prev_after   BIGINT;
    prev_version BIGINT;
    w_currency   CHAR(3);
BEGIN
    SELECT currency INTO w_currency FROM wallets WHERE id = NEW.wallet_id;
    IF w_currency IS DISTINCT FROM NEW.currency THEN
        RAISE EXCEPTION 'moeda do lançamento (%) difere da carteira (%)', NEW.currency, w_currency
            USING ERRCODE = 'check_violation';
    END IF;

    SELECT balance_after_minor, wallet_version INTO prev_after, prev_version
      FROM wallet_ledger_entries
     WHERE wallet_id = NEW.wallet_id
     ORDER BY wallet_version DESC
     LIMIT 1;

    IF prev_version IS NOT NULL AND NEW.wallet_version <= prev_version THEN
        RAISE EXCEPTION 'versão % não é posterior à última (%)', NEW.wallet_version, prev_version
            USING ERRCODE = 'check_violation';
    END IF;
    IF COALESCE(prev_after, 0) <> NEW.balance_before_minor THEN
        RAISE EXCEPTION 'balance_before % não encadeia com o saldo anterior %', NEW.balance_before_minor, COALESCE(prev_after, 0)
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER ledger_chain_check
    BEFORE INSERT ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_check_chain();

-- ---------------------------------------------------------------------------
-- Proteções da carteira
-- ---------------------------------------------------------------------------
-- Identidade imutável; saldo só muda com incremento de exatamente 1 na versão;
-- versão não muda sem mudança de saldo (impede lost update silencioso).
CREATE FUNCTION wallets_guard_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.id <> OLD.id OR NEW.player_id <> OLD.player_id OR NEW.currency <> OLD.currency
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'identidade da carteira é imutável' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.balance_minor <> OLD.balance_minor AND NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'mudança de saldo exige versão % (recebida %)', OLD.version + 1, NEW.version
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.balance_minor = OLD.balance_minor AND NEW.version <> OLD.version THEN
        RAISE EXCEPTION 'versão só pode mudar junto com o saldo' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wallets_guard_update
    BEFORE UPDATE ON wallets
    FOR EACH ROW EXECUTE FUNCTION wallets_guard_update();

CREATE FUNCTION wallets_forbid_delete() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'carteiras não podem ser excluídas' USING ERRCODE = 'integrity_constraint_violation';
END;
$$;

CREATE TRIGGER wallets_forbid_delete
    BEFORE DELETE ON wallets
    FOR EACH ROW EXECUTE FUNCTION wallets_forbid_delete();

-- Todo saldo diferente do estado inicial vazio precisa de um lançamento na mesma
-- versão com o mesmo saldo final, confirmado na mesma transação SQL (checagem
-- adiada para o COMMIT).
CREATE FUNCTION wallets_require_ledger() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    cur_balance BIGINT;
    cur_version BIGINT;
BEGIN
    -- Relê o estado final da linha no momento do COMMIT.
    SELECT balance_minor, version INTO cur_balance, cur_version FROM wallets WHERE id = NEW.id;
    IF cur_version = 1 AND cur_balance = 0 THEN
        RETURN NULL;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM wallet_ledger_entries
         WHERE wallet_id = NEW.id AND wallet_version = cur_version AND balance_after_minor = cur_balance
    ) THEN
        RAISE EXCEPTION 'carteira % (v%) sem lançamento correspondente no ledger', NEW.id, cur_version
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER wallets_require_ledger
    AFTER INSERT OR UPDATE ON wallets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wallets_require_ledger();

-- ---------------------------------------------------------------------------
-- Proteções das transações
-- ---------------------------------------------------------------------------
-- Campos de identidade e de negócio são imutáveis; estados terminais não mudam;
-- transições respeitam a máquina de estados.
CREATE FUNCTION wager_tx_guard_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'transação % está em estado terminal %', OLD.id, OLD.status
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.id <> OLD.id OR NEW.origin <> OLD.origin OR NEW.kind <> OLD.kind
       OR NEW.wallet_id <> OLD.wallet_id OR NEW.player_id <> OLD.player_id
       OR NEW.amount_minor <> OLD.amount_minor OR NEW.currency <> OLD.currency
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.external_transaction_id IS DISTINCT FROM OLD.external_transaction_id
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
       OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
       OR NEW.round_id IS DISTINCT FROM OLD.round_id
       OR NEW.game_id IS DISTINCT FROM OLD.game_id
       OR NEW.reference_external_transaction_id IS DISTINCT FROM OLD.reference_external_transaction_id
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'campos de negócio da transação são imutáveis' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.status = 'PENDING' AND OLD.status <> 'PENDING' THEN
        RAISE EXCEPTION 'transição % -> PENDING inválida', OLD.status USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wager_tx_guard_update
    BEFORE UPDATE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_tx_guard_update();

CREATE TRIGGER wager_tx_forbid_delete
    BEFORE DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION ledger_forbid_mutation();

-- ---------------------------------------------------------------------------
-- Outbox transacional
-- ---------------------------------------------------------------------------
CREATE TABLE outbox_events (
    event_id        UUID        PRIMARY KEY,
    aggregate_type  TEXT        NOT NULL,
    aggregate_id    UUID        NOT NULL,
    event_type      TEXT        NOT NULL,
    event_version   INTEGER     NOT NULL CHECK (event_version >= 1),
    partition_key   TEXT        NOT NULL,
    correlation_id  TEXT,
    -- JSON (e não JSONB) preserva exatamente os bytes do snapshot serializado.
    payload         JSON        NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts        INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    published_at    TIMESTAMPTZ,
    last_error      TEXT
);

CREATE INDEX outbox_unpublished_idx ON outbox_events (next_attempt_at, occurred_at) WHERE published_at IS NULL;

-- O snapshot do evento é imutável; apenas os campos de controle de entrega mudam.
CREATE FUNCTION outbox_guard_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.event_id <> OLD.event_id OR NEW.aggregate_type <> OLD.aggregate_type
       OR NEW.aggregate_id <> OLD.aggregate_id OR NEW.event_type <> OLD.event_type
       OR NEW.event_version <> OLD.event_version OR NEW.partition_key <> OLD.partition_key
       OR NEW.payload::text <> OLD.payload::text OR NEW.occurred_at <> OLD.occurred_at THEN
        RAISE EXCEPTION 'snapshot do evento % é imutável', OLD.event_id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
        RAISE EXCEPTION 'evento % já publicado', OLD.event_id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER outbox_guard_update
    BEFORE UPDATE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_guard_update();

-- ---------------------------------------------------------------------------
-- Inbox (deduplicação de mensagens por consumidor)
-- ---------------------------------------------------------------------------
CREATE TABLE inbox_messages (
    consumer_name  TEXT        NOT NULL,
    message_id     TEXT        NOT NULL,
    message_hash   CHAR(64)    NOT NULL,
    transaction_id UUID        REFERENCES wager_transactions (id),
    received_at    TIMESTAMPTZ NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL,
    CONSTRAINT inbox_messages_pk PRIMARY KEY (consumer_name, message_id)
);

-- ---------------------------------------------------------------------------
-- Privilégios mínimos para o papel da aplicação
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO wallet_app;
GRANT SELECT, INSERT, UPDATE ON wallets TO wallet_app;
GRANT SELECT, INSERT, UPDATE ON wager_transactions TO wallet_app;
GRANT SELECT, INSERT ON wallet_ledger_entries TO wallet_app;
GRANT SELECT, INSERT, UPDATE ON outbox_events TO wallet_app;
GRANT SELECT, INSERT ON inbox_messages TO wallet_app;
