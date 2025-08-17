# Implemented in Stage 3
import pandas as pd
import numpy as np

# ---------- helpers for indicators ----------

def calc_return_1d(close: pd.Series) -> pd.Series:
    """
    Simple 1-day return: (close / close.shift(1) - 1)
    """
    return close.pct_change()

def calc_sma(series: pd.Series, window: int = 20) -> pd.Series:
    """
    Simple moving average over a rolling window.
    """
    return series.rolling(window=window, min_periods=window).mean()

def calc_ema(series: pd.Series, span: int = 20) -> pd.Series:
    """
    Exponential moving average with adjust=False (more common for indicators).
    """
    return series.ewm(span=span, adjust=False).mean()

def calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """
    RSI using Wilder's method.
    - Compute price changes
    - Separate gains/losses
    - Use EMA (alpha=1/period) for avg gain/loss
    - RSI = 100 - (100 / (1 + RS))
    """
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi

# ---------- main pipeline ----------

def apply_transformations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich the tidy prices DataFrame with:
      - data types
      - per-symbol sorting
      - return_1d
      - sma_20, ema_20
      - rsi_14
    Leaves initial NA values for warm-up windows (expected).
    """

    if df.empty:
        return df

    # Ensure correct dtypes / column names we expect
    expected_cols = ["symbol", "trade_date", "open", "high", "low", "close", "adj_close", "volume", "source"]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"apply_transformations: missing columns {missing}")

    out = df.copy()

    # Normalize dtypes
    out["symbol"] = out["symbol"].astype(str)
    # Ensure datetime tz-aware -> convert to date for DB daily granularity later
    out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.tz_localize(None)

    numeric_cols = ["open", "high", "low", "close", "adj_close", "volume"]
    for c in numeric_cols:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    # Sort so rolling calculations make sense
    out = out.sort_values(["symbol", "trade_date"]).reset_index(drop=True)

    # Group by symbol for all rolling calcs
    def _per_symbol(g: pd.DataFrame) -> pd.DataFrame:
        g = g.copy()
        g["return_1d"] = calc_return_1d(g["close"])
        g["sma_20"]    = calc_sma(g["close"], window=20)
        g["ema_20"]    = calc_ema(g["close"], span=20)
        g["rsi_14"]    = calc_rsi(g["close"], period=14)
        return g

    out = out.groupby("symbol", group_keys=False).apply(_per_symbol)

    # Optional: handle tiny gaps — here we leave warm-up NaNs as-is (transparent to users).
    # If you prefer to fill small gaps, uncomment:
    # out[["return_1d", "sma_20", "ema_20", "rsi_14"]] = (
    #     out[["return_1d", "sma_20", "ema_20", "rsi_14"]].fillna(method="ffill")
    # )

    # Final column order used by loader later
    cols = ["symbol", "trade_date", "open", "high", "low", "close", "adj_close", "volume",
            "return_1d", "sma_20", "ema_20", "rsi_14", "source"]
    return out[cols]
