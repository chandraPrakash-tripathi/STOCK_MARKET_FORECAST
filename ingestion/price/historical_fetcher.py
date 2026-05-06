# ingestion/price/historical_fetcher.py

import yfinance as yf
import pandas as pd
from sqlalchemy import text
from loguru import logger
from scripts.config import WATCHLIST
from scripts.db_connection import get_db


def fetch_ticker_data(ticker: str) -> pd.DataFrame:
    """
    Downloads 10y OHLCV data for a single ticker.
    Returns a clean DataFrame with columns matching our DB schema.
    """
    logger.info(f"Fetching data for {ticker}...")

    df = yf.download(ticker, period="10y", auto_adjust=True, progress=False)

    if df.empty:
        logger.warning(f"No data returned for {ticker}")
        return pd.DataFrame()

    # Flatten MultiIndex columns if present
    # yfinance sometimes returns ('Close', 'RELIANCE.NS') style columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Reset index so Date becomes a regular column
    df = df.reset_index()

    # Normalize all column names to lowercase
    df.columns = [col.lower() for col in df.columns]
    # now: date | open | high | low | close | volume

    # Add ticker column
    df["ticker"] = ticker

    # auto_adjust=True means Close is already adjusted
    df["adj_close"] = df["close"]

    # Convert pandas Timestamp → Python date (SQLAlchemy needs this)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Drop rows with missing close price
    df = df.dropna(subset=["close"])

    return df[["ticker", "date", "open", "high", "low", "close", "volume", "adj_close"]]


def upsert_to_db(session, df: pd.DataFrame, ticker: str) -> tuple[int, int]:
    """
    Upserts rows into raw.ohlcv_historical using SQLAlchemy session.
    Returns (inserted_count, skipped_count).
    """
    if df.empty:
        return 0, 0

    # Using SQLAlchemy text() with named :param style
    # ON CONFLICT DO NOTHING = safe to re-run anytime
    sql = text("""
        INSERT INTO raw.ohlcv_historical 
            (ticker, date, open, high, low, close, volume, adj_close)
        VALUES 
            (:ticker, :date, :open, :high, :low, :close, :volume, :adj_close)
        ON CONFLICT (ticker, date) DO NOTHING
    """)

    rows = df.to_dict(orient="records")
    # [{"ticker": "RELIANCE.NS", "date": date(2015,1,1), "open": 100.0, ...}, ...]

    inserted = 0
    skipped = 0

    for row in rows:
        result = session.execute(sql, row)
        # rowcount = 1 → inserted, 0 → conflict hit (DO NOTHING)
        if result.rowcount == 1:
            inserted += 1
        else:
            skipped += 1

    # NOTE: No session.commit() here!
    # get_db() context manager handles commit/rollback automatically

    return inserted, skipped


def run():
    """
    Main orchestrator — loops over WATCHLIST, fetches and upserts each ticker.
    """
    logger.info(f"Starting historical ingestion for {len(WATCHLIST)} tickers")
    logger.info(f"Watchlist: {WATCHLIST}")

    total_inserted = 0
    total_skipped = 0
    failed_tickers = []

    # get_db() is a @contextmanager — must use 'with' to get the session
    # it auto-commits on success, auto-rollbacks on exception
    with get_db() as session:
        for ticker in WATCHLIST:
            try:
                df = fetch_ticker_data(ticker)
                inserted, skipped = upsert_to_db(session, df, ticker)

                total_inserted += inserted
                total_skipped += skipped

                logger.info(
                    f"{ticker}: {inserted} rows inserted, {skipped} rows skipped"
                )

            except Exception as e:
                # One bad ticker won't kill the whole pipeline
                logger.error(f"Failed to process {ticker}: {e}")
                failed_tickers.append(ticker)
                continue

    logger.info("=" * 50)
    logger.info(f"Ingestion complete.")
    logger.info(f"Total inserted : {total_inserted}")
    logger.info(f"Total skipped  : {total_skipped}")
    if failed_tickers:
        logger.warning(f"Failed tickers : {failed_tickers}")


if __name__ == "__main__":
    run()
