# ingestion/kaggle/yfinance_gap_fill.py
#
# Fills raw.kaggle_nifty50 from 2021-05-01 → today using yfinance.
# Reads the symbol list directly from what's already in the DB.
# Safe to re-run — ON CONFLICT DO NOTHING.

import yfinance as yf
import pandas as pd
from datetime import date
from sqlalchemy import text
from loguru import logger
from scripts.db_connection import get_db

GAP_START = date(2021, 5, 1)
GAP_END = date.today()  # always fills up to today on re-runs

# NSE suffix map  — symbols in the Kaggle table have no suffix,
# but yfinance needs ".NS" to hit NSE data.
# A handful of names differ between Kaggle and yfinance — handled below.
SYMBOL_SUFFIX = ".NS"

# Kaggle symbol → yfinance ticker overrides (only where they differ)
YFINANCE_OVERRIDES = {
    "MM": "M&M.NS",
    "INFRATEL": "INFRATEL.NS",  # delisted; yfinance may return empty — handled gracefully
}

UPSERT_SQL = text("""
    INSERT INTO raw.kaggle_nifty50 (
        symbol, series, trade_date, prev_close,
        open, high, low, close, vwap,
        volume, turnover, trades,
        deliverable_vol, pct_deliverable
    )
    VALUES (
        :symbol, :series, :trade_date, :prev_close,
        :open, :high, :low, :close, :vwap,
        :volume, :turnover, :trades,
        :deliverable_vol, :pct_deliverable
    )
    ON CONFLICT (symbol, trade_date) DO NOTHING
""")


def get_symbols_from_db(session) -> list[str]:
    """Pull the distinct symbol list from what Kaggle ingestion already loaded."""
    result = session.execute(
        text("SELECT DISTINCT symbol FROM raw.kaggle_nifty50 ORDER BY symbol")
    )
    symbols = [row[0] for row in result.fetchall()]
    logger.info(f"Found {len(symbols)} symbols in DB: {symbols[:5]} ...")
    return symbols


def fetch_yfinance(kaggle_symbol: str) -> pd.DataFrame:
    """
    Downloads OHLCV for one symbol from yfinance for the gap period.
    Maps yfinance columns → kaggle_nifty50 schema.
    Returns empty DataFrame if no data.
    """
    ticker_str = YFINANCE_OVERRIDES.get(
        kaggle_symbol, f"{kaggle_symbol}{SYMBOL_SUFFIX}"
    )

    df = yf.download(
        ticker_str,
        start=GAP_START.isoformat(),
        end=GAP_END.isoformat(),
        auto_adjust=True,  # Close = adjusted close, consistent with Kaggle data
        progress=False,
    )

    if df.empty:
        return pd.DataFrame()

    # Flatten MultiIndex if present (yfinance quirk)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()
    df.columns = [c.lower() for c in df.columns]
    # columns now: date | open | high | low | close | volume

    df["symbol"] = kaggle_symbol  # store WITHOUT .NS — matches Kaggle
    df["series"] = "EQ"  # NSE equity series
    df["trade_date"] = pd.to_datetime(df["date"]).dt.date
    df["prev_close"] = df["close"].shift(1)  # best approximation
    df["vwap"] = None  # not available from yfinance
    df["turnover"] = None
    df["trades"] = None
    df["deliverable_vol"] = None
    df["pct_deliverable"] = None

    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

    return df[
        [
            "symbol",
            "series",
            "trade_date",
            "prev_close",
            "open",
            "high",
            "low",
            "close",
            "vwap",
            "volume",
            "turnover",
            "trades",
            "deliverable_vol",
            "pct_deliverable",
        ]
    ].dropna(subset=["close", "trade_date"])


def upsert(session, df: pd.DataFrame) -> tuple[int, int]:
    rows = df.to_dict(orient="records")
    clean_rows = [
        {k: (None if pd.isna(v) else v) for k, v in row.items()} for row in rows
    ]
    inserted = skipped = 0
    for row in clean_rows:
        result = session.execute(UPSERT_SQL, row)
        if result.rowcount == 1:
            inserted += 1
        else:
            skipped += 1
    return inserted, skipped


def run():
    logger.info("=== yfinance gap-fill starting ===")
    logger.info(f"Gap window : {GAP_START} → {GAP_END}")

    with get_db() as session:
        symbols = get_symbols_from_db(session)

        total_inserted = total_skipped = 0
        failed = []

        for symbol in symbols:
            try:
                df = fetch_yfinance(symbol)

                if df.empty:
                    logger.warning(f"  {symbol}: no yfinance data (possibly delisted)")
                    continue

                inserted, skipped = upsert(session, df)
                total_inserted += inserted
                total_skipped += skipped
                logger.info(
                    f"  {symbol}: {inserted} inserted, {skipped} skipped ({len(df)} rows fetched)"
                )

            except Exception as e:
                logger.error(f"  {symbol}: FAILED — {e}")
                failed.append(symbol)

    logger.info("=" * 50)
    logger.info(f"Gap-fill complete.")
    logger.info(f"  Inserted : {total_inserted:,}")
    logger.info(f"  Skipped  : {total_skipped:,}  (already existed)")
    if failed:
        logger.warning(f"  Failed   : {failed}")
    logger.info("=" * 50)


if __name__ == "__main__":
    run()
