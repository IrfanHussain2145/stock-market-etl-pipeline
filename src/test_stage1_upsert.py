from datetime import date
from src.db import upsert_securities, upsert_prices_daily

def main():
    # 1. Insert a security (AAPL) into market.securities
    result1 = upsert_securities([
        ("AAPL", "Apple Inc.", "NASDAQ", "USD", "Technology", "Consumer Electronics")
    ])
    print("Securities upsert:", result1)

    # 2. Insert one price row for AAPL on 2024-12-31
    rows_v1 = [
        ("AAPL", date(2024, 12, 31), 190.00, 192.00, 189.00, 191.00, 191.00, 50000000,
         0.0123, 190.50, 190.70, 55.0, "seed")
    ]
    print("Prices v1:", upsert_prices_daily(rows_v1))

    # 3. Insert again with different values → should UPDATE not duplicate
    rows_v2 = [
        ("AAPL", date(2024, 12, 31), 195.00, 197.00, 194.00, 196.00, 196.00, 51000000,
         0.0180, 191.00, 191.50, 57.0, "seed-update")
    ]
    print("Prices v2 (update):", upsert_prices_daily(rows_v2))

    print("Done. Check DB to confirm values were updated, not duplicated.")

if __name__ == "__main__":
    main()
