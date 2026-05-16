# # File: ingestion/price/live_fetcher.py
# # What it does:
# # Runs during market hours only (check: 9:15 AM to 3:30 PM IST, weekdays)
# # Every 5 minutes, calls yf.download(ticker, period="1d", interval="5m") for each watchlist ticker
# # Upserts into raw.ohlcv_live
# # If market is closed, exits cleanly with a log message
# ingestion/price/live_fetcher.py
import yfinance as yf
from datetime import datetime, time
import pytz
import pandas as pd
from loguru import logger
from sqlalchemy import text
import time as time_module
from scripts.db_connection import get_db

IST = pytz.timezone("Asia/Kolkata")


# ── A) Market hours check ────────────────────────────────────────────────────


def is_market_open() -> bool:
    curr_ist = datetime.now(IST)
    if curr_ist.weekday() >= 6:  # Saturday=5, Sunday=6
        return False
    market_open = time(9, 15)
    market_close = time(15, 30)  # NSE closes 3:30 PM, not 6:30 PM
    return market_open <= curr_ist.time() <= market_close


# ── B) Dynamic watchlist from kaggle_nifty50 ────────────────────────────────


def get_watchlist_from_db(session) -> list[str]:
    """
    Pull every distinct symbol from raw.kaggle_nifty50 and append .NS
    so yfinance resolves them as NSE tickers.
    Returns e.g. ['RELIANCE.NS', 'TCS.NS', 'INFY.NS', ...]
    """
    result = session.execute(text("""
        SELECT DISTINCT symbol
        FROM raw.kaggle_nifty50
        ORDER BY symbol
    """))
    rows = result.fetchall()

    if not rows:
        logger.error("No symbols found in raw.kaggle_nifty50 — is the table populated?")
        return []

    tickers = [f"{row[0]}.NS" for row in rows]
    logger.info(f"Watchlist loaded: {len(tickers)} tickers from kaggle_nifty50")
    return tickers


# ── C) Fetch one ticker ──────────────────────────────────────────────────────


def fetch_live_data(ticker: str) -> pd.DataFrame:
    df = yf.download(
        ticker,
        period="1d",
        interval="5m",
        auto_adjust=True,
        progress=False,
    )

    if df.empty:
        logger.warning(f"{ticker}: empty response from yfinance")
        return pd.DataFrame()

    # flatten MultiIndex columns (yfinance sometimes returns these)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()
    df.columns = [col.lower() for col in df.columns]
    df["ticker"] = ticker

    # convert bar timestamp to IST-aware datetime
    df["timestamp"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(IST)
    df = df.dropna(subset=["close"])

    return df[["ticker", "timestamp", "open", "high", "low", "close", "volume"]]


# ── D) Upsert into raw.ohlcv_live ───────────────────────────────────────────


def upsert_live(session, df: pd.DataFrame) -> tuple[int, int]:
    if df.empty:
        return 0, 0

    sql = text("""
        INSERT INTO raw.ohlcv_live (ticker, timestamp, open, high, low, close, volume)
        VALUES (:ticker, :timestamp, :open, :high, :low, :close, :volume)
        ON CONFLICT (ticker, timestamp) DO NOTHING
    """)

    rows = df.to_dict(orient="records")
    inserted, skipped = 0, 0

    for row in rows:
        result = session.execute(sql, row)
        if result.rowcount == 1:
            inserted += 1
        else:
            skipped += 1

    session.commit()
    return inserted, skipped


# ── E) One fetch-upsert cycle ────────────────────────────────────────────────


def run_cycle(session, watchlist: list[str]) -> None:
    logger.info(f"── Cycle start: {datetime.now(IST).strftime('%H:%M:%S')} IST ──")

    total_inserted, total_skipped = 0, 0

    for ticker in watchlist:
        try:
            df = fetch_live_data(ticker)
            ins, skip = upsert_live(session, df)
            total_inserted += ins
            total_skipped += skip
            logger.info(f"{ticker}: {ins} inserted, {skip} skipped")
        except Exception as e:
            logger.error(f"{ticker}: failed — {e}")
            session.rollback()
            continue

    logger.info(f"Cycle done — {total_inserted} inserted, {total_skipped} skipped")


# ── F) Entrypoint ────────────────────────────────────────────────────────────


def run() -> None:
    logger.info("live_fetcher starting")

    if not is_market_open():
        logger.info("Market closed — exiting cleanly")
        return

    with get_db() as session:
        # load watchlist once per session, not per cycle
        watchlist = get_watchlist_from_db(session)
        if not watchlist:
            logger.error("Empty watchlist — aborting")
            return

        while is_market_open():
            run_cycle(session, watchlist)
            logger.info(
                f"Sleeping 300s — next cycle at "
                f"{datetime.now(IST).strftime('%H:%M:%S')} IST"
            )
            time_module.sleep(300)

    logger.info("Market closed — exiting cleanly")


if __name__ == "__main__":
    run()
