-- Provisionamento inicial do PostgreSQL (executado apenas na primeira inicialização do volume).
--
-- Separação de papéis:
--   wallet_owner -> dono do schema; usado somente pelas migrations.
--   wallet_app   -> usado pela aplicação em tempo de execução; recebe apenas os
--                   privilégios mínimos concedidos pelas migrations (por exemplo,
--                   nenhum UPDATE/DELETE/TRUNCATE no ledger).
-- As senhas abaixo são valores locais de exemplo, não segredos reais.

CREATE ROLE wallet_owner LOGIN PASSWORD 'wallet_owner';
CREATE ROLE wallet_app LOGIN PASSWORD 'wallet_app';

CREATE DATABASE wallet OWNER wallet_owner;
CREATE DATABASE wallet_test OWNER wallet_owner;

REVOKE ALL ON DATABASE wallet FROM PUBLIC;
REVOKE ALL ON DATABASE wallet_test FROM PUBLIC;
GRANT CONNECT ON DATABASE wallet TO wallet_owner, wallet_app;
GRANT CONNECT ON DATABASE wallet_test TO wallet_owner, wallet_app;
