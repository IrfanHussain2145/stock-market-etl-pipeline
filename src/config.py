# Loaded in Stage 2/3
from dataclasses import dataclass
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

@dataclass
class Settings:
    db_user: str = os.getenv("DB_USER", "market")
    db_pass: str = os.getenv("DB_PASSWORD", "market")
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "5433"))
    db_name: str = os.getenv("DB_NAME", "marketdb")
    
    tickers: list = None
    start_date: str = os.getenv("START_DATE", "2020-01-01")
    end_date: str = os.getenv("END_DATE", "2050-01-01")

    def __post_init__(self):
        tickers_str = os.getenv("TICKERS", "AAPL")
        self.tickers = [t.strip().upper() for t in tickers_str.split(",") if t.strip()]

settings = Settings()
