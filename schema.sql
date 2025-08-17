-- Stage 1 will add CREATE SCHEMA/TABLES here.
CREATE SCHEMA IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.securities (
  symbol        TEXT PRIMARY KEY,
  name          TEXT,
  exchange      TEXT,
  currency      TEXT,
  sector        TEXT,
  industry      TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS market.prices_daily (
  symbol        TEXT NOT NULL,
  trade_date    DATE NOT NULL,
  open          NUMERIC(18,6),
  high          NUMERIC(18,6),
  low           NUMERIC(18,6),
  close         NUMERIC(18,6),
  adj_close     NUMERIC(18,6),
  volume        BIGINT,
  return_1d     NUMERIC(18,8),
  sma_20        NUMERIC(18,6),
  ema_20        NUMERIC(18,6),
  rsi_14        NUMERIC(18,6),
  source        TEXT NOT NULL,
  loaded_at     TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (symbol, trade_date),
  FOREIGN KEY (symbol) REFERENCES market.securities(symbol)
);

CREATE INDEX IF NOT EXISTS idx_prices_daily_symbol_date ON market.prices_daily(symbol, trade_date);
CREATE INDEX IF NOT EXISTS idx_prices_daily_date ON market.prices_daily(trade_date);

CREATE TABLE IF NOT EXISTS market.pipeline_runs (
  run_id      BIGSERIAL PRIMARY KEY,
  run_started TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  run_finished TIMESTAMPTZ,
  status      TEXT NOT NULL,              -- 'success' | 'warning' | 'error'
  message     TEXT
);
