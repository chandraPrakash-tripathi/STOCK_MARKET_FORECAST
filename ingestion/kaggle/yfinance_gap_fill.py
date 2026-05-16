# ingestion/kaggle/yfinance_gap_fill.py
#
# Fills raw.kaggle_nifty50 from 2021-05-01 → today using yfinance.
# Reads the symbol list directly from what's already in the DB.
# Safe to re-run — ON CONFLICT DO NOTHING.

import io
import math
import yfinance as yf
import pandas as pd
from datetime import date
from sqlalchemy import text
from loguru import logger
from scripts.db_connection import get_db

GAP_START = date(2021, 5, 1)
GAP_END = date.today()

SYMBOL_SUFFIX = ".NS"

YFINANCE_OVERRIDES = {
    "MM": "M&M.NS",
    "INFRATEL": "INFRATEL.NS",  # delisted — yfinance may return empty
}

DB_COLS = [
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


# ── Step 1: symbols ───────────────────────────────────────────────────────────


def get_symbols_from_db(session) -> list[str]:
    result = session.execute(
        text("SELECT DISTINCT symbol FROM raw.kaggle_nifty50 ORDER BY symbol")
    )
    symbols = [row[0] for row in result.fetchall()]
    logger.info(f"Found {len(symbols)} symbols in DB: {symbols[:5]} ...")
    return symbols


# ── Step 2: fetch all symbols in one yfinance call ────────────────────────────


def fetch_all_yfinance(symbols: list[str]) -> pd.DataFrame:
    """
    Downloads all symbols in a single yf.download() call — one HTTP round trip
    instead of 50. Much faster than per-symbol loops.
    """
    # Build ticker list
    tickers = [YFINANCE_OVERRIDES.get(s, f"{s}{SYMBOL_SUFFIX}") for s in symbols]
    # Keep a reverse map: yfinance ticker → kaggle symbol
    ticker_to_symbol = {
        YFINANCE_OVERRIDES.get(s, f"{s}{SYMBOL_SUFFIX}"): s for s in symbols
    }

    logger.info(f"Downloading {len(tickers)} tickers from yfinance in one batch...")
    raw = yf.download(
        tickers,
        start=GAP_START.isoformat(),
        end=GAP_END.isoformat(),
        auto_adjust=True,
        progress=False,
        group_by="ticker",  # MultiIndex: (field, ticker)
    )

    if raw.empty:
        logger.warning("yfinance returned no data at all")
        return pd.DataFrame()

    all_frames = []
    failed = []

    for ticker, kaggle_symbol in ticker_to_symbol.items():
        try:
            # Slice this ticker's columns out of the MultiIndex DataFrame
            if ticker in raw.columns.get_level_values(0):
                df = raw[ticker].copy()
            else:
                logger.warning(f"  {kaggle_symbol} ({ticker}): not in response")
                continue

            df = df.dropna(how="all")
            if df.empty:
                logger.warning(f"  {kaggle_symbol}: no data (possibly delisted)")
                continue

            df = df.reset_index()
            df.columns = [str(c).lower() for c in df.columns]
            # columns: date | open | high | low | close | volume

            df["symbol"] = kaggle_symbol
            df["series"] = "EQ"
            df["trade_date"] = pd.to_datetime(df["date"]).dt.date
            df["prev_close"] = df["close"].shift(1)
            df["vwap"] = None
            df["turnover"] = None
            df["trades"] = None
            df["deliverable_vol"] = None
            df["pct_deliverable"] = None

            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

            df = df[DB_COLS].dropna(subset=["close", "trade_date"])
            all_frames.append(df)
            logger.debug(f"  {kaggle_symbol}: {len(df):,} rows")

        except Exception as e:
            logger.error(f"  {kaggle_symbol}: parse error — {e}")
            failed.append(kaggle_symbol)

    if failed:
        logger.warning(f"Failed to parse: {failed}")

    if not all_frames:
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["symbol", "trade_date"])
    logger.info(f"Total rows ready for upsert: {len(combined):,}")
    return combined


# ── Step 3: bulk upsert via COPY + temp table ─────────────────────────────────


def upsert(session, df: pd.DataFrame) -> tuple[int, int]:
    """
    COPY entire DataFrame into a temp table in one TCP stream,
    then INSERT ... ON CONFLICT DO NOTHING from temp → real table.
    """
    cols = [c for c in DB_COLS if c in df.columns]
    df_out = df[cols].copy()

    # psycopg2 can't handle pandas Int64 NA — convert to Python None
    for col in ["volume", "trades", "deliverable_vol"]:
        if col in df_out.columns:
            df_out[col] = (
                df_out[col].astype(object).where(df_out[col].notna(), other=None)
            )

    # Serialize to in-memory TSV; NA → \N (PostgreSQL NULL marker)
    buf = io.StringIO()
    df_out.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
    buf.seek(0)

    raw_conn = session.connection().connection

    with raw_conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE tmp_yf_gap
                (LIKE raw.kaggle_nifty50 INCLUDING DEFAULTS)
            ON COMMIT DROP
        """)

        cur.copy_from(
            buf,
            "tmp_yf_gap",
            sep="\t",
            null="\\N",
            columns=cols,
        )
        logger.info(f"COPY → temp table: {len(df_out):,} rows")

        col_list = ", ".join(cols)
        cur.execute(f"""
            INSERT INTO raw.kaggle_nifty50 ({col_list})
            SELECT {col_list} FROM tmp_yf_gap
            ON CONFLICT (symbol, trade_date) DO NOTHING
        """)
        inserted = cur.rowcount
        skipped = len(df_out) - inserted

    raw_conn.commit()
    return inserted, skipped


# ── Entrypoint ────────────────────────────────────────────────────────────────


def run():
    logger.info("=== yfinance gap-fill starting ===")
    logger.info(f"Gap window : {GAP_START} → {GAP_END}")

    with get_db() as session:
        symbols = get_symbols_from_db(session)

    combined_df = fetch_all_yfinance(symbols)

    if combined_df.empty:
        logger.warning("No data fetched — nothing to upsert.")
        return

    with get_db() as session:
        inserted, skipped = upsert(session, combined_df)

    logger.info("=" * 50)
    logger.info("Gap-fill complete.")
    logger.info(f"  Inserted : {inserted:,}")
    logger.info(f"  Skipped  : {skipped:,}  (already existed)")
    logger.info("=" * 50)


if __name__ == "__main__":
    run()
