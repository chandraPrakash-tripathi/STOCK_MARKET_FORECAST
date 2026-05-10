# ingestion/kaggle/kaggle_fetcher.py

import kagglehub
import pandas as pd
from pathlib import Path
from sqlalchemy import text
from loguru import logger
from scripts.db_connection import get_db

DATASET = "rohanrao/nifty50-stock-market-data"

COLUMN_MAP = {
    "Date": "trade_date",
    "Symbol": "symbol",
    "Series": "series",
    "Prev Close": "prev_close",
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "VWAP": "vwap",
    "Volume": "volume",
    "Turnover": "turnover",
    "Trades": "trades",
    "Deliverable Volume": "deliverable_vol",
    "%Deliverble": "pct_deliverable",  # Kaggle typo — kept as-is
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


def download_dataset() -> Path:
    """
    Downloads the full dataset zip to kagglehub local cache.
    Returns the local directory path where CSVs are extracted.
    On re-runs it just returns the cached path — no re-download.
    """
    logger.info(f"Calling kagglehub.dataset_download for '{DATASET}'...")
    path = kagglehub.dataset_download(DATASET)
    local = Path(path)
    logger.info(f"Dataset available at: {local}")
    return local


def load_all_stock_files(dataset_dir: Path) -> pd.DataFrame:
    """
    Reads every per-stock CSV (skips metadata.csv) from the local cache dir.
    Concatenates into one DataFrame.
    """
    csv_files = sorted(dataset_dir.glob("*.csv"))
    stock_files = [f for f in csv_files if f.name.lower() != "metadata.csv"]
    logger.info(f"Found {len(stock_files)} stock CSV files (+ metadata skipped)")

    all_frames = []
    failed = []

    for f in stock_files:
        try:
            df = pd.read_csv(f)
            # Some files don't include Symbol column — derive from filename
            if "Symbol" not in df.columns:
                df["Symbol"] = f.stem  # e.g. "RELIANCE" from RELIANCE.csv
            all_frames.append(df)
            logger.info(f"  {f.name}: {len(df):,} rows")
        except Exception as e:
            logger.warning(f"  Could not read {f.name}: {e}")
            failed.append(f.name)

    if failed:
        logger.warning(f"Skipped {len(failed)} files: {failed}")

    combined = pd.concat(all_frames, ignore_index=True)
    logger.info(f"Total rows loaded: {len(combined):,}")
    return combined


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns, cast types, drop bad rows."""
    df = df.rename(columns=COLUMN_MAP)

    # Keep only known columns
    keep = [c for c in COLUMN_MAP.values() if c in df.columns]
    df = df[keep]

    # Parse date
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date

    # Numeric coercion
    numeric_cols = [
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
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Nullable integers
    for col in ["volume", "trades", "deliverable_vol"]:
        if col in df.columns:
            df[col] = df[col].astype("Int64")

    before = len(df)
    df = df.dropna(subset=["close", "trade_date", "symbol"])
    dropped = before - len(df)
    if dropped:
        logger.warning(f"Dropped {dropped} rows with null close/date/symbol")

    df = df.drop_duplicates(subset=["symbol", "trade_date"])
    logger.info(f"Clean rows ready: {len(df):,}")
    logger.info(f"Date range : {df['trade_date'].min()} → {df['trade_date'].max()}")
    logger.info(f"Symbols    : {sorted(df['symbol'].unique())[:5]} ...")
    return df


def upsert(session, df: pd.DataFrame) -> tuple[int, int]:
    """Bulk upsert into raw.kaggle_nifty50. Returns (inserted, skipped)."""
    rows = df.to_dict(orient="records")

    # Replace pandas NA/NaT with None → SQL NULL
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
    logger.info("=== Kaggle NIFTY-50 ingestion starting ===")

    logger.info("Step 1/3  Downloading dataset (cached after first run)...")
    dataset_dir = download_dataset()

    logger.info("Step 2/3  Loading and cleaning CSVs...")
    raw_df = load_all_stock_files(dataset_dir)
    clean_df = clean(raw_df)

    logger.info("Step 3/3  Upserting into raw.kaggle_nifty50...")
    with get_db() as session:
        inserted, skipped = upsert(session, clean_df)

    logger.info("=" * 50)
    logger.info(f"Ingestion complete.")
    logger.info(f"  Inserted : {inserted:,}")
    logger.info(f"  Skipped  : {skipped:,}  (already existed — safe to re-run)")
    logger.info("=" * 50)


if __name__ == "__main__":
    run()
