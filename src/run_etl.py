import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from src.db import start_run, finish_run
from src.extract_yfinance import fetch_prices
from src.transform import apply_transformations
from src.load import load_prices
from src.config import settings

# --- logging setup: console + rotating file ---
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "etl.log"

logger = logging.getLogger("etl")
logger.setLevel(logging.INFO)

# console
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

# rotating file (1 MB x 5 files)
fh = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=5)
fh.setLevel(logging.INFO)
fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

# avoid duplicate handlers if re-run in same interpreter
if not logger.handlers:
    logger.addHandler(ch)
    logger.addHandler(fh)

def main():
    run_id = start_run(status="running", message="ETL started")
    try:
        logger.info("Starting ETL run: EXTRACT → TRANSFORM → LOAD")
        logger.info(f"Tickers={settings.tickers} | Range={settings.start_date}→{settings.end_date}")

        raw = fetch_prices()
        logger.info(f"Extracted rows: {len(raw)}")
        if raw.empty:
            msg = "No data extracted; exiting early."
            logger.warning(msg)
            finish_run(run_id, status="warning", message=msg)
            return

        tx = apply_transformations(raw)
        logger.info(f"Transformed rows: {len(tx)}")
        nulls = tx[["return_1d", "sma_20", "ema_20", "rsi_14"]].isna().sum().to_dict()
        logger.info(f"Null counts (expected warm-up windows): {nulls}")

        res = load_prices(tx)
        msg = f"Load result: {res}"
        logger.info(msg)
        finish_run(run_id, status="success", message=msg)
        logger.info("ETL run complete.")
    except Exception as e:
        logger.exception("ETL run failed")
        finish_run(run_id, status="error", message=str(e))
        raise

if __name__ == "__main__":
    main()
