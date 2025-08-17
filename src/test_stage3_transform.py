from src.extract_yfinance import fetch_prices
from src.transform import apply_transformations

def main():
    raw = fetch_prices()
    print("Extracted:", len(raw))
    tx = apply_transformations(raw)
    print("Transformed:", len(tx))
    print(tx.tail(5))  # peek last 5 rows to see indicators filled

if __name__ == "__main__":
    main()
