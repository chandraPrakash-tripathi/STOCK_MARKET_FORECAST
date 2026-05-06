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

            # OHLCV Historical
            create_table(
                db,
                """
                CREATE TABLE IF NOT EXISTS raw.ohlcv_historical (
                    id BIGSERIAL PRIMARY KEY,
                    ticker VARCHAR(20) NOT NULL,
                    date DATE NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume BIGINT,
                    adj_close DOUBLE PRECISION,
                    ingested_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(ticker, date)
                );
            """,
                "ohlcv_historical",
            )

            # OHLCV Live
            create_table(
                db,
                """
                CREATE TABLE IF NOT EXISTS raw.ohlcv_live (
                    id BIGSERIAL PRIMARY KEY,
                    ticker VARCHAR(20) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume BIGINT,
                    ingested_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(ticker, timestamp)
                );
            """,
                "ohlcv_live",
            )

            # Bhavcopy
            create_table(
                db,
                """
                CREATE TABLE IF NOT EXISTS raw.bhavcopy (
                    id BIGSERIAL PRIMARY KEY,
                    ticker VARCHAR(20),
                    date DATE,
                    series VARCHAR(5),
                    prev_close DOUBLE PRECISION,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    traded_quantity BIGINT,
                    deliverable_quantity BIGINT,
                    delivery_pct DOUBLE PRECISION,
                    ingested_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(ticker, date)
                );
            """,
                "bhavcopy",
            )

            # FII DII Flows
            create_table(
                db,
                """
                CREATE TABLE IF NOT EXISTS raw.fii_dii_flows (
                    id BIGSERIAL PRIMARY KEY,
                    date DATE NOT NULL UNIQUE,
                    fii_buy_cash DOUBLE PRECISION,
                    fii_sell_cash DOUBLE PRECISION,
                    fii_net_cash DOUBLE PRECISION,
                    dii_buy_cash DOUBLE PRECISION,
                    dii_sell_cash DOUBLE PRECISION,
                    dii_net_cash DOUBLE PRECISION,
                    fii_net_fo DOUBLE PRECISION,
                    ingested_at TIMESTAMP DEFAULT NOW()
                );
            """,
                "fii_dii_flows",
            )

            # Nifty PCR
            create_table(
                db,
                """
                CREATE TABLE IF NOT EXISTS raw.nifty_pcr (
                    id BIGSERIAL PRIMARY KEY,
                    date DATE NOT NULL UNIQUE,
                    pcr_oi DOUBLE PRECISION,
                    pcr_volume DOUBLE PRECISION,
                    total_call_oi BIGINT,
                    total_put_oi BIGINT,
                    ingested_at TIMESTAMP DEFAULT NOW()
                );
            """,
                "nifty_pcr",
            )

            # Macro
            create_table(
                db,
                """
                CREATE TABLE IF NOT EXISTS raw.macro (
                    id BIGSERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    close DOUBLE PRECISION,
                    ingested_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(date, symbol)
                );
            """,
                "macro",
            )

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

            # News Enriched
            create_table(
                db,
                """
                CREATE TABLE IF NOT EXISTS raw.news_enriched (
                    id BIGSERIAL PRIMARY KEY,
                    article_id BIGINT REFERENCES raw.news_articles(id),
                    ticker VARCHAR(20),
                    sentiment_score DOUBLE PRECISION,
                    sentiment_label VARCHAR(10),
                    summary TEXT,
                    processed_at TIMESTAMP DEFAULT NOW()
                );
            """,
                "news_enriched",
            )

            logger.info("Database setup completed successfully")

    except Exception as e:
        logger.error(f"NOT ABLE TO RUN THE QUERIES: {e}")


if __name__ == "__main__":
    init_db()
