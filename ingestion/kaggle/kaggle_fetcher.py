import io
import math
import kagglehub
import pandas as pd
from pathlib import Path
from loguru import logger
from scripts.db_connection import get_db

DATASET = "rohanrao/nifty50-stock-market-data"

# NIFTY50_all.csv already has all 50 stocks — individual CSVs are duplicates
MASTER_FILE = "NIFTY50_all.csv"

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

# Ordered exactly as the table DDL — used for COPY and INSERT column list
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


def _is_null(v) -> bool:
    """Safe null check — avoids TypeError on datetime.date objects."""
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    return False


# ── Step 1: Download ──────────────────────────────────────────────────────────


def download_dataset() -> Path:
    """
    Downloads dataset to kagglehub local cache (~/.cache/kagglehub/...).
    On re-runs returns the cached path immediately — no re-download.
    """
    logger.info(f"kagglehub.dataset_download → '{DATASET}'")
    path = kagglehub.dataset_download(DATASET)
    local = Path(path)
    logger.info(f"Dataset available at: {local}")
    return local


# ── Step 2: Load ──────────────────────────────────────────────────────────────


def load_master_file(dataset_dir: Path) -> pd.DataFrame:
    """
    Loads only NIFTY50_all.csv — the pre-combined master file that already
    contains every per-stock row. Individual stock CSVs are exact duplicates
    and are intentionally skipped to avoid doubling the row count.
    """
    master = dataset_dir / MASTER_FILE
    if not master.exists():
        raise FileNotFoundError(
            f"Master file not found: {master}\n"
            f"Files present: {[f.name for f in dataset_dir.glob('*.csv')]}"
        )

    logger.info(f"Loading {master.name} ...")
    df = pd.read_csv(master)
    logger.info(f"Rows loaded: {len(df):,}")
    return df


# ── Step 3: Clean ─────────────────────────────────────────────────────────────


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns, cast types, drop/deduplicate bad rows."""
    df = df.rename(columns=COLUMN_MAP)

    # Keep only columns we know about
    keep = [c for c in COLUMN_MAP.values() if c in df.columns]
    df = df[keep].copy()

    # Date
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date

    # Numerics
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

    # Nullable integers — pandas Int64 preserves NA without converting to float
    for col in ["volume", "trades", "deliverable_vol"]:
        if col in df.columns:
            df[col] = df[col].astype("Int64")

    # Drop rows missing critical fields
    before = len(df)
    df = df.dropna(subset=["close", "trade_date", "symbol"])
    dropped = before - len(df)
    if dropped:
        logger.warning(f"Dropped {dropped:,} rows (null close / date / symbol)")

    # Deduplicate — keeps first occurrence
    df = df.drop_duplicates(subset=["symbol", "trade_date"])

    logger.info(f"Clean rows ready : {len(df):,}")
    logger.info(
        f"Date range       : {df['trade_date'].min()} → {df['trade_date'].max()}"
    )
    logger.info(f"Symbols (sample) : {sorted(df['symbol'].unique())[:5]} ...")
    return df


# ── Step 4: Upsert ────────────────────────────────────────────────────────────


def upsert(session, df: pd.DataFrame) -> tuple[int, int]:
    """
    Fastest possible bulk upsert via PostgreSQL COPY + temp table strategy:

      1. COPY entire DataFrame into a throw-away temp table (single TCP stream)
      2. INSERT ... SELECT ... ON CONFLICT DO NOTHING from temp → real table
      3. Temp table is auto-dropped at end of transaction (ON COMMIT DROP)

    ~100x faster than SQLAlchemy row-by-row or bulk execute() for ON CONFLICT.
    """
    # Only keep columns that actually exist in the cleaned df, in DDL order
    cols = [c for c in DB_COLS if c in df.columns]
    df_out = df[cols].copy()

    # psycopg2 can't handle pandas Int64 NA — convert to Python None
    for col in ["volume", "trades", "deliverable_vol"]:
        if col in df_out.columns:
            df_out[col] = (
                df_out[col].astype(object).where(df_out[col].notna(), other=None)
            )

    # Serialize to in-memory tab-separated buffer; NA → \N (PostgreSQL NULL)
    buf = io.StringIO()
    df_out.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
    buf.seek(0)

    # Get the raw psycopg2 connection from the SQLAlchemy session
    raw_conn = session.connection().connection

    with raw_conn.cursor() as cur:
        # 1. Temp table — auto-dropped when transaction ends
        cur.execute("""
            CREATE TEMP TABLE tmp_kaggle_nifty50
                (LIKE raw.kaggle_nifty50 INCLUDING DEFAULTS)
            ON COMMIT DROP
        """)

        # 2. Stream entire buffer in one COPY call
        cur.copy_from(
            buf,
            "tmp_kaggle_nifty50",
            sep="\t",
            null="\\N",
            columns=cols,
        )
        logger.info(f"COPY → temp table: {len(df_out):,} rows transferred")

        # 3. Merge temp → real table, skip existing (symbol, trade_date) pairs
        col_list = ", ".join(cols)
        cur.execute(f"""
            INSERT INTO raw.kaggle_nifty50 ({col_list})
            SELECT {col_list}
            FROM tmp_kaggle_nifty50
            ON CONFLICT (symbol, trade_date) DO NOTHING
        """)
        inserted = cur.rowcount
        skipped = len(df_out) - inserted

    raw_conn.commit()
    return inserted, skipped


# ── Entrypoint ────────────────────────────────────────────────────────────────


def run():
    logger.info("=== Kaggle NIFTY-50 ingestion starting ===")

    logger.info("Step 1/3  Downloading dataset (cached after first run)...")
    dataset_dir = download_dataset()

    logger.info("Step 2/3  Loading and cleaning master CSV...")
    raw_df = load_master_file(dataset_dir)
    clean_df = clean(raw_df)

    logger.info("Step 3/3  Upserting into raw.kaggle_nifty50 via COPY...")
    with get_db() as session:
        inserted, skipped = upsert(session, clean_df)

    logger.info("=" * 50)
    logger.info("Ingestion complete.")
    logger.info(f"  Inserted : {inserted:,}")
    logger.info(f"  Skipped  : {skipped:,}  (already existed — safe to re-run)")
    logger.info("=" * 50)


if __name__ == "__main__":
    run()
