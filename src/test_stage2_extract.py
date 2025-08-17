from src.extract_yfinance import fetch_prices
from src.config import settings

def main():
    df = fetch_prices()
    print("Rows fetched:", len(df))
    print(df.head())

if __name__ == "__main__":
    main()
