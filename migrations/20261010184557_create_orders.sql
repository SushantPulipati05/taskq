-- +goose Up
-- A small "business" table so Taskq can demonstrate the transactional outbox:
-- an order and the job that emails its confirmation are saved in ONE transaction.
CREATE TABLE orders (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_email text        NOT NULL,
    -- why: money is stored as an integer number of paise, never a float.
    -- 0.1 + 0.2 != 0.3 in floating point; 10 + 20 == 30 in integers.
    amount_paise   bigint      NOT NULL CHECK (amount_paise > 0),
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS orders;