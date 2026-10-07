-- Reversão do schema inicial. ATENÇÃO: remove todos os dados financeiros;
-- destinada a ambientes locais e de teste.
DROP TABLE IF EXISTS inbox_messages;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS wallet_ledger_entries;
DROP TABLE IF EXISTS wager_transactions;
DROP TABLE IF EXISTS wallets;

DROP FUNCTION IF EXISTS outbox_guard_update();
DROP FUNCTION IF EXISTS wager_tx_guard_update();
DROP FUNCTION IF EXISTS wallets_require_ledger();
DROP FUNCTION IF EXISTS wallets_forbid_delete();
DROP FUNCTION IF EXISTS wallets_guard_update();
DROP FUNCTION IF EXISTS ledger_check_chain();
DROP FUNCTION IF EXISTS ledger_forbid_mutation();
