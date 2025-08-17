# Implemented in Stage 4
# src/load.py
from typing import List, Tuple, Dict, Any
import math
import pandas as pd
from src.db import upsert_securities, upsert_prices_daily

def _none_if_nan(v):
    """Convert NaN/NaT to None so psycopg2 can insert NULLs."""
    if v is None:
        return None
    # pandas uses NaN/NaT which are not equal to themselves
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass
    return v

def _df_to_price_rows(df: pd.DataFrame) -> List[Tuple]:
    """
    Map tidy DataFrame to the exact column order required by prices_daily:
    (symbol, trade_date, open, high, low, close, adj_close, volume,
     return_1d, sma_20, ema_20, rsi_14, source)
    """
    rows: List[Tuple] = []
    for _, r in df.iterrows():
        trade_date = pd.to_datetime(r["trade_date"]).date()  # DATE column in DB
        rows.append((
            str(r["symbol"]),
            trade_date,
            _none_if_nan(float(r["open"])) if r["open"] is not None else None,
            _none_if_nan(float(r["high"])) if r["high"] is not None else None,
            _none_if_nan(float(r["low"])) if r["low"] is not None else None,
            _none_if_nan(float(r["close"])) if r["close"] is not None else None,
            _none_if_nan(float(r["adj_close"])) if r["adj_close"] is not None else None,
            _none_if_nan(int(r["volume"])) if pd.notna(r["volume"]) else None,
            _none_if_nan(float(r["return_1d"])) if pd.notna(r["return_1d"]) else None,
            _none_if_nan(float(r["sma_20"])) if pd.notna(r["sma_20"]) else None,
            _none_if_nan(float(r["ema_20"])) if pd.notna(r["ema_20"]) else None,
            _none_if_nan(float(r["rsi_14"])) if pd.notna(r["rsi_14"]) else None,
            str(r["source"]),
        ))
    return rows

def _ensure_securities(df: pd.DataFrame) -> Dict[str, int]:
    """
    Upsert stub securities for each unique symbol so FK succeeds.
    Other columns (name, exchange, etc.) left NULL for now.
    """
    symbols = sorted(set(df["symbol"].astype(str).tolist()))
    sec_rows = [(sym, None, None, None, None, None) for sym in symbols]
    return upsert_securities(sec_rows)

def load_prices(df: pd.DataFrame) -> Dict[str, int]:
    """
    Ensure securities exist, then upsert prices. Returns counts.
    """
    if df.empty:
        return {"securities_upserts": 0, "price_upserts": 0}
    sec_res = _ensure_securities(df)
    price_rows = _df_to_price_rows(df)
    price_res = upsert_prices_daily(price_rows)
    return {
        "securities_upserts": sec_res.get("upserts", 0),
        "price_upserts": price_res.get("upserts", 0),
    }
