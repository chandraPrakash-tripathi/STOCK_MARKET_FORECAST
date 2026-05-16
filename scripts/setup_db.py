from .db_connection import get_db
from loguru import logger
from sqlalchemy import text


def verify_table(db, table_name: str):
    result = db.execute(text(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'raw'
        AND table_name = '{table_name}';
    """))

    if result.fetchone():
        logger.info(f"{table_name} verified")
    else:
        logger.error(f"{table_name} verification failed")


def create_table(db, sql: str, table_name: str):
    db.execute(text(sql))
    logger.info(f"{table_name} created successfully")
    verify_table(db, table_name)


def init_db():
    try:
        with get_db() as db:

            # Create schema
            db.execute(text("""
                CREATE SCHEMA IF NOT EXISTS raw;
            """))

            logger.info("Schema created successfully")

            # Verify schema
            result = db.execute(text("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = 'raw';
            """))

            if result.fetchone():
                logger.info("Schema verified")
            else:
                logger.error("Schema verification failed")
                return

            # News Articles
            create_table(
                db,
                """
                CREATE TABLE IF NOT EXISTS raw.news_articles (
                    id BIGSERIAL PRIMARY KEY,
                    source VARCHAR(100),
                    title TEXT,
                    description TEXT,
                    url TEXT UNIQUE,
                    published_at TIMESTAMP,
                    raw_text TEXT,
                    ingested_at TIMESTAMP DEFAULT NOW()
                );
            """,
                "news_articles",
            )

            # kaggle
            create_table(
                db,
                """
                CREATE TABLE IF NOT EXISTS raw.kaggle_nifty50 (
                    id              BIGSERIAL PRIMARY KEY,
                    symbol          VARCHAR(30)    NOT NULL,
                    series          VARCHAR(10),
                    trade_date      DATE           NOT NULL,
                    prev_close      NUMERIC(12, 4),
                    open            NUMERIC(12, 4),
                    high            NUMERIC(12, 4),
                    low             NUMERIC(12, 4),
                    close           NUMERIC(12, 4) NOT NULL,
                    vwap            NUMERIC(12, 4),
                    volume          BIGINT,
                    turnover        NUMERIC(20, 4),
                    trades          BIGINT,
                    deliverable_vol BIGINT,
                    pct_deliverable NUMERIC(8, 4),
                    ingested_at     TIMESTAMP DEFAULT NOW(),
                    UNIQUE (symbol, trade_date)
                );
                """,
                "kaggle_nifty50",
            )
            create_table(
                db,
                """
                CREATE TABLE IF NOT EXISTS raw.ohlcv_live (
                    id          BIGSERIAL PRIMARY KEY,
                    ticker      VARCHAR(30)    NOT NULL,          -- NSE ticker with .NS suffix
                    timestamp   TIMESTAMPTZ    NOT NULL,          -- bar open time, IST-aware
                    open        NUMERIC(12, 4),
                    high        NUMERIC(12, 4),
                    low         NUMERIC(12, 4),
                    close       NUMERIC(12, 4) NOT NULL,
                    volume      BIGINT,
                    ingested_at TIMESTAMPTZ    DEFAULT NOW(),
                    UNIQUE (ticker, timestamp)                    -- upsert key
                );
                """,
                "ohlcv_live",
            )

            logger.info("Database setup completed successfully")

    except Exception as e:
        logger.error(f"NOT ABLE TO RUN THE QUERIES: {e}")


if __name__ == "__main__":
    init_db()
