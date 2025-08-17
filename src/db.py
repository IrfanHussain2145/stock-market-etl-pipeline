import os
from typing import Iterable, Tuple
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# Load environment variables from .env
load_dotenv()

def get_conn():
    """
    Create and return a PostgreSQL connection using credentials
    from environment variables.
    """
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME", "marketdb"),
        user=os.getenv("DB_USER", "market"),
        password=os.getenv("DB_PASSWORD", "market"),
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5433")),  # host port mapped in docker-compose
    )

def upsert_securities(rows: Iterable[Tuple[str, str, str, str, str, str]]):
    """
    Insert or update rows in the market.securities table.
    Each row should be a tuple of:
        (symbol, name, exchange, currency, sector, industry)
    
    - If the symbol is new → it is inserted.
    - If the symbol already exists → its fields are updated.
    """
    if not rows:
        return {"upserts": 0}

    sql = """
        INSERT INTO market.securities (symbol, name, exchange, currency, sector, industry)
        VALUES %s
        ON CONFLICT (symbol) DO UPDATE SET
            name = EXCLUDED.name,
            exchange = EXCLUDED.exchange,
            currency = EXCLUDED.currency,
            sector = EXCLUDED.sector,
            industry = EXCLUDED.industry;
    """

    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            # execute_values efficiently inserts many rows at once
            execute_values(cur, sql, rows)
        return {"upserts": len(rows)}
    finally:
        conn.close()

def upsert_prices_daily(rows):
    if not rows:
        return {"upserts": 0}

    sql = """
        INSERT INTO market.prices_daily (
            symbol, trade_date, open, high, low, close, adj_close, volume,
            return_1d, sma_20, ema_20, rsi_14, source
        )
        VALUES %s
        ON CONFLICT (symbol, trade_date)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low  = EXCLUDED.low,
            close = EXCLUDED.close,
            adj_close = EXCLUDED.adj_close,
            volume = EXCLUDED.volume,
            return_1d = EXCLUDED.return_1d,
            sma_20 = EXCLUDED.sma_20,
            ema_20 = EXCLUDED.ema_20,
            rsi_14 = EXCLUDED.rsi_14,
            source = EXCLUDED.source,
            loaded_at = NOW();
    """
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            from psycopg2.extras import execute_values
            execute_values(cur, sql, rows)
        return {"upserts": len(rows)}
    finally:
        conn.close()

def start_run(status: str = "running", message: str = None) -> int:
    """
    Insert a pipeline_runs row and return its run_id.
    """
    sql = "INSERT INTO market.pipeline_runs(status, message) VALUES (%s, %s) RETURNING run_id;"
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, (status, message))
            run_id = cur.fetchone()[0]
        return run_id
    finally:
        conn.close()

def finish_run(run_id: int, status: str, message: str = None):
    """
    Mark a run as finished with final status and optional message.
    """
    sql = """
        UPDATE market.pipeline_runs
        SET run_finished = NOW(), status = %s, message = %s
        WHERE run_id = %s;
    """
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, (status, message, run_id))
    finally:
        conn.close()
