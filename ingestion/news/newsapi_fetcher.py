# ingestion/news/newsapi_fetcher.py

from newsapi import NewsApiClient
from scripts.config import NEWS_API_KEY, WATCHLIST
from scripts.db_connection import get_db
from sqlalchemy import text
from datetime import datetime, timezone, timedelta
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────
newsapi = NewsApiClient(api_key=NEWS_API_KEY)

HOURS = 72
LANGUAGE = "en"
SORT_BY = "publishedAt"
PAGE_SIZE = 100

TICKER_NAME_MAP = {
    "RELIANCE.NS": "Reliance Industries",
    "TCS.NS": "Tata Consultancy Services",
    "HDFCBANK.NS": "HDFC Bank",
    "INFY.NS": "Infosys",
    "ICICIBANK.NS": "ICICI Bank",
    "SBIN.NS": "State Bank of India",
    "WIPRO.NS": "Wipro",
    "AXISBANK.NS": "Axis Bank",
    "KOTAKBANK.NS": "Kotak Mahindra Bank",
    "BAJFINANCE.NS": "Bajaj Finance",
}


# ── Query builder ─────────────────────────────────────────────────────────────
def build_query() -> str:
    """Build NewsAPI OR query from watchlist tickers."""
    names = [TICKER_NAME_MAP.get(t, t.replace(".NS", "")) for t in WATCHLIST if t]
    query = " OR ".join(f'"{name}"' for name in names)
    logger.info(f"NewsAPI query: {query}")
    return query


# ── Fetch ─────────────────────────────────────────────────────────────────────
def fetch_articles(hours: int = HOURS) -> list[dict]:
    from_dt = datetime.now(timezone.utc) - timedelta(hours=hours)
    from_str = from_dt.strftime("%Y-%m-%dT%H:%M:%S")

    response = newsapi.get_everything(
        q=build_query(),
        language=LANGUAGE,
        sort_by=SORT_BY,
        from_param=from_str,
        page_size=PAGE_SIZE,
        page=1,
    )

    articles = response.get("articles", [])
    logger.info(f"NewsAPI returned {len(articles)} articles since {from_str}")
    return articles


# ── Deduplicate (within batch only) ───────────────────────────────────────────
def deduplicate(articles: list[dict]) -> list[dict]:
    seen, unique = set(), []
    for a in articles:
        url = (a.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        unique.append(a)
    logger.info(f"{len(unique)} unique articles in this batch")
    return unique


# ── Transform ─────────────────────────────────────────────────────────────────
def transform(article: dict) -> dict:
    source = article.get("source") or {}

    raw_published = article.get("publishedAt") or ""
    try:
        published_at = datetime.fromisoformat(raw_published.replace("Z", "+00:00"))
    except ValueError:
        published_at = None

    return {
        "source": source.get("name"),
        "title": article.get("title"),
        "description": article.get("description"),
        "url": (article.get("url") or "").strip(),
        "published_at": published_at,
        "raw_text": article.get("content"),
    }


# ── Upsert ────────────────────────────────────────────────────────────────────
UPSERT_SQL = text("""
    INSERT INTO raw.news_articles
        (source, title, description, url, published_at, raw_text)
    VALUES
        (:source, :title, :description, :url, :published_at, :raw_text)
    ON CONFLICT (url)
    DO UPDATE SET
        title       = EXCLUDED.title,
        description = EXCLUDED.description,
        raw_text    = EXCLUDED.raw_text,
        source      = EXCLUDED.source;
""")


def upsert_articles(rows: list[dict]) -> int:
    if not rows:
        logger.info("Nothing to upsert.")
        return 0
    with get_db() as db:
        db.execute(UPSERT_SQL, rows)
    logger.info(f"Upserted {len(rows)} rows into raw.news_articles")
    return len(rows)


# ── Orchestrator ──────────────────────────────────────────────────────────────
def run(hours: int = HOURS) -> None:
    logger.info("── NewsAPI fetcher starting ──")
    articles = fetch_articles(hours)
    unique = deduplicate(articles)
    rows = [transform(a) for a in unique]
    upsert_articles(rows)
    logger.info("── NewsAPI fetcher done ──")


if __name__ == "__main__":
    run()
