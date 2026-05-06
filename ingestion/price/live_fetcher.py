# # File: ingestion/price/live_fetcher.py
# # What it does:
# # Runs during market hours only (check: 9:15 AM to 3:30 PM IST, weekdays)
# # Every 5 minutes, calls yf.download(ticker, period="1d", interval="5m") for each watchlist ticker
# # Upserts into raw.ohlcv_live
# # If market is closed, exits cleanly with a log message
import yfinance as yf
from datetime import datetime, time
import pytz
import pandas as pd
from scripts.config import WATCHLIST
from loguru import logger
from sqlalchemy import text
import time as time_module
from scripts.db_connection import get_db

IST = pytz.timezone("Asia/Kolkata")


def is_market_open() -> bool:
    # step 1: get current IST time
    curr_ist = datetime.now(IST)
    # step 2: check if weekend
    if curr_ist.weekday() >= 5:
        return False
    # step 3: define market_open/close
    market_open = time(9, 15)
    market_close = time(18, 30)
    # step 4: check time range
    if market_open <= curr_ist.time() <= market_close:
        return True
    return False


#  B) fetch_live_data().
def fetch_live_data(ticker: str) -> pd.DataFrame:
    # 1. yf.download(ticker, period="1d", interval="5m", ...)
    df = yf.download(
        ticker, period="1d", interval="5m", auto_adjust=True, progress=False
    )
    # 2. if empty → log warning, return pd.DataFrame()
    if df.empty:
        logger.warning("data is empty")
        # return df ## never
        return pd.DataFrame()
    # 3. flatten MultiIndex
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    # 4. reset_index
    df = df.reset_index()
    # 5. lowercase columns
    df.columns = [col.lower() for col in df.columns]
    # 6. add ticker column
    df["ticker"] = ticker
    # 7. convert datetime → IST, store as "timestamp"
    df["timestamp"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert(IST)
    # 8. dropna on close
    df = df.dropna(subset=["close"])
    # 9. return only the columns the DB needs
    return df[["ticker", "timestamp", "open", "high", "low", "close", "volume"]]


# C)upsert_live
def upsert_live(session, df: pd.DataFrame) -> tuple[int, int]:
    # 1. if df empty → return (0, 0)
    if df.empty:
        return 0, 0
    # 2. write the sql using text("""...""")
    #    INSERT INTO raw.ohlcv_live (ticker, timestamp, ...)
    #    VALUES (:ticker, :timestamp, ...)
    #    ON CONFLICT (ticker, timestamp) DO NOTHING
    sql = text("""
         INSERT INTO raw.ohlcv_live (ticker, timestamp, open, high, low, close, volume)
        VALUES (:ticker, :timestamp, :open, :high, :low, :close, :volume)
        ON CONFLICT (ticker, timestamp) DO NOTHING
         """)

    # 3. convert df → list of dicts using to_dict("records")
    rows = df.to_dict(orient="records")

    # 4. loop over rows, execute each, track inserted/skipped via rowcount
    inserted, skipped = 0, 0
    for row in rows:
        result = session.execute(sql, row)
        if result.rowcount == 1:  # 1 = inserted, 0 = skipped
            inserted += 1
        else:
            skipped += 1
    # 5. return (inserted, skipped)
    session.commit()
    return inserted, skipped


# D) run_cycle
def run_cycle(session) -> None:
    logger.info(
        f"── Cycle start: {datetime.now(IST).strftime('%H:%M:%S')} IST ──"
    )  # log start time

    total_inserted, total_skipped = 0, 0  # initialise counters

    for ticker in WATCHLIST:  # loop over what?
        try:
            df = fetch_live_data(ticker)  # which function fetches data?
            ins, skip = upsert_live(session, df)  # which function upserts?
            total_inserted += ins
            total_skipped += skip
            logger.info(
                f"{ticker}: {ins} inserted, {skip} skipped"
            )  # log per ticker result
        except Exception as e:
            logger.error(f"Failed: {ticker}: {e}")
            session.rollback()
            continue

    logger.info(
        f"Cycle done — {total_inserted} inserted, {total_skipped} skipped"
    )  # log cycle summary


# E) run
def run() -> None:
    logger.info("starting")  # log "starting..."

    if not is_market_open():  # check market hours
        logger.info("Market closed")  # log "market closed"
        return  # exit cleanly

    with get_db() as session:  # open DB session
        while is_market_open():  # loop while market is open
            run_cycle(session)  # run one cycle
            logger.info("Sleeping")  # log "sleeping..."
            time_module.sleep(300)  # sleep 5 minutes

    logger.info("exiting cleanly")  # log "exiting cleanly"


if __name__ == "__main__":
    run()
