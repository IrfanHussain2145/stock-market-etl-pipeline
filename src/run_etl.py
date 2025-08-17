import logging
from datetime import datetime
from src.extract_yfinance import fetch_prices
from src.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def main():
    logging.info("Starting ETL run (EXTRACT only for Stage 2)")
    logging.info(f"Tickers={settings.tickers} | Range={settings.start_date}→{settings.end_date}")

    df = fetch_prices()
    logging.info(f"Extracted rows: {len(df)}")

    if len(df) > 0:
        # show a tiny sample for sanity
        sample = df.head(3).to_dict(orient="records")
        logging.info(f"Sample rows: {sample}")

    logging.info("Stage 2 extract substep complete.")

if __name__ == "__main__":
    main()
