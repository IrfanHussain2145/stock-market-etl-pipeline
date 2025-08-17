import time
from typing import List, Optional
import pandas as pd
import yfinance as yf
from src.config import settings

def _fetch_one(symbol: str, start: str, end: str) -> pd.DataFrame:
    """
    Fetch one symbol using Ticker.history (more reliable than download).
    Falls back to yf.download if needed. Returns tidy DF or empty DF.
    """
    # Try Ticker.history first
    try:
        tkr = yf.Ticker(symbol)
        df = tkr.history(start=start, end=end, interval="1d", actions=False, auto_adjust=False)
        if not df.empty:
            df = df.reset_index().rename(columns={
                "Date": "trade_date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Adj Close": "adj_close",
            })
            df["symbol"] = symbol
            df["source"] = "yfinance"
            df = df[["symbol", "trade_date", "open", "high", "low", "close", "adj_close", "volume", "source"]]
            return df
    except Exception as e:
        print(f"[history] {symbol} failed: {e}")

    # Fallback to yf.download
    try:
        df = yf.download(symbol, start=start, end=end, progress=False, interval="1d", actions=False, threads=False)
        if not df.empty:
            df = df.reset_index().rename(columns={
                "Date": "trade_date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Adj Close": "adj_close",
            })
            df["symbol"] = symbol
            df["source"] = "yfinance"
            df = df[["symbol", "trade_date", "open", "high", "low", "close", "adj_close", "volume", "source"]]
            return df
    except Exception as e:
        print(f"[download] {symbol} failed: {e}")

    # If both paths failed
    return pd.DataFrame()

def fetch_prices(tickers: Optional[List[str]] = None, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch historical daily stock prices for given tickers and date range.
    Returns tidy DataFrame with columns:
    symbol, trade_date, open, high, low, close, adj_close, volume, source
    Includes simple retry per ticker to mitigate transient Yahoo hiccups.
    """
    tickers = tickers or settings.tickers
    start = start or settings.start_date
    end = end or settings.end_date

    frames = []
    for sym in tickers:
        df = _fetch_one(sym, start, end)
        if df.empty:
            # simple retry after short sleep
            time.sleep(1.0)
            df = _fetch_one(sym, start, end)
        if df.empty:
            print(f"[warn] No data returned for {sym}")
        else:
            frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
