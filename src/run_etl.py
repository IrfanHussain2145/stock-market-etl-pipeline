import logging
from src.extract_yfinance import fetch_prices
from src.transform import apply_transformations
from src.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def main():
    logging.info("Starting ETL run: EXTRACT → TRANSFORM")
    logging.info(f"Tickers={settings.tickers} | Range={settings.start_date}→{settings.end_date}")

    # EXTRACT
    raw = fetch_prices()
    logging.info(f"Extracted rows: {len(raw)}")

    if raw.empty:
        logging.warning("No data extracted; exiting early.")
        return

    # TRANSFORM
    tx = apply_transformations(raw)
    logging.info(f"Transformed rows: {len(tx)}")
    # quick null diagnostics on new features
    nulls = tx[["return_1d", "sma_20", "ema_20", "rsi_14"]].isna().sum().to_dict()
    logging.info(f"Null counts (expected warm-up windows): {nulls}")

    # Show a tiny sample
    sample = tx.head(3).to_dict(orient="records")
    logging.info(f"Sample rows: {sample}")

    logging.info("Stage 3 complete (extract + transform).")

if __name__ == "__main__":
    main()
