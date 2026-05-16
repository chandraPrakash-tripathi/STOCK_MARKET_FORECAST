# ingestion/news/newsapi_fetcher.py

from newsapi import NewsApiClient
from scripts.config import NEWS_API_KEY
from scripts.db_connection import get_db
from sqlalchemy import text
from datetime import datetime, timezone, timedelta
from loguru import logger
import re

# ── Config ────────────────────────────────────────────────────────────────────
newsapi = NewsApiClient(api_key=NEWS_API_KEY)

HOURS = 72
LANGUAGE = "en"
SORT_BY = "publishedAt"
PAGE_SIZE = 100
QUERY_CHUNK_SIZE = 10

# Known good overrides — for symbols where the raw ticker is ambiguous or
# the canonical news name differs significantly from the symbol
TICKER_NAME_OVERRIDES: dict[str, str] = {
    "RELIANCE": "Reliance Industries",
    "TCS": "Tata Consultancy Services",
    "HDFCBANK": "HDFC Bank",
    "INFY": "Infosys",
    "ICICIBANK": "ICICI Bank",
    "SBIN": "State Bank of India",
    "WIPRO": "Wipro",
    "AXISBANK": "Axis Bank",
    "KOTAKBANK": "Kotak Mahindra Bank",
    "BAJFINANCE": "Bajaj Finance",
    "BAJAJFINSV": "Bajaj Finserv",
    "HINDUNILVR": "Hindustan Unilever",
    "BHARTIARTL": "Bharti Airtel",
    "ASIANPAINT": "Asian Paints",
    "HCLTECH": "HCL Technologies",
    "MARUTI": "Maruti Suzuki",
    "SUNPHARMA": "Sun Pharmaceutical",
    "TATAMOTORS": "Tata Motors",
    "TATASTEEL": "Tata Steel",
    "TITAN": "Titan Company",
    "ULTRACEMCO": "UltraTech Cement",
    "POWERGRID": "Power Grid Corporation",
    "NTPC": "NTPC Limited",
    "ONGC": "Oil and Natural Gas Corporation",
    "COALINDIA": "Coal India",
    "JSWSTEEL": "JSW Steel",
    "ADANIPORTS": "Adani Ports",
    "ADANIENT": "Adani Enterprises",
    "DRREDDY": "Dr Reddys Laboratories",
    "CIPLA": "Cipla",
    "DIVISLAB": "Divi's Laboratories",
    "APOLLOHOSP": "Apollo Hospitals",
    "EICHERMOT": "Eicher Motors",
    "HEROMOTOCO": "Hero MotoCorp",
    "BPCL": "Bharat Petroleum",
    "IOC": "Indian Oil Corporation",
    "GRASIM": "Grasim Industries",
    "TECHM": "Tech Mahindra",
    "LT": "Larsen and Toubro",
    "NESTLEIND": "Nestle India",
    "BRITANNIA": "Britannia Industries",
    "UPL": "UPL Limited",
    "SBILIFE": "SBI Life Insurance",
    "HDFCLIFE": "HDFC Life Insurance",
    "ICICIGI": "ICICI Lombard",
    "INDUSINDBK": "IndusInd Bank",
    "BAJAJ-AUTO": "Bajaj Auto",
    "M&M": "Mahindra and Mahindra",
    "WIPRO": "Wipro",
    "TATACONSUM": "Tata Consumer Products",
}


# ── Validation ────────────────────────────────────────────────────────────────
def is_valid_search_term(term: str) -> bool:
    """
    Validate that a resolved name is actually useful for news search.
    Rejects terms that are:
      - too short to be meaningful (single char, or pure numbers)
      - look like unresolved tickers (all caps, no spaces, >5 chars)
      - contain only special characters
    """
    term = term.strip()
    if not term or len(term) < 3:
        return False
    if re.fullmatch(r"[A-Z0-9&.\-]{4,}", term):
        # Still looks like a raw ticker — usable but flag it
        logger.warning(f"Search term looks like an unresolved ticker: '{term}'")
        return True  # keep it — NSE symbols do appear in news
    if not re.search(r"[a-zA-Z]", term):  # no letters at all
        return False
    return True


# ── Load + resolve tickers from DB ───────────────────────────────────────────
def load_search_terms() -> list[str]:
    """
    1. Fetch DISTINCT symbols from raw.kaggle_nifty50
    2. Strip exchange suffixes (.NS / .BO / .BSE)
    3. Resolve to a human-readable name via TICKER_NAME_OVERRIDES,
       or fall back to the raw base symbol
    4. Validate each term before keeping it
    """
    with get_db() as db:
        rows = db.execute(
            text("SELECT DISTINCT symbol FROM raw.kaggle_nifty50 ORDER BY symbol;")
        ).fetchall()

    symbols = [row[0] for row in rows]
    logger.info(f"Found {len(symbols)} distinct symbols in kaggle_nifty50")

    terms: list[str] = []
    skipped: list[str] = []

    for symbol in symbols:
        # Strip exchange suffix if present (e.g. RELIANCE.NS → RELIANCE)
        base = symbol.split(".")[0].upper().strip()

        # Resolve to a proper name or keep the base symbol
        resolved = TICKER_NAME_OVERRIDES.get(base, base)

        if is_valid_search_term(resolved):
            terms.append(resolved)
        else:
            skipped.append(symbol)

    if skipped:
        logger.warning(
            f"Skipped {len(skipped)} symbols that failed validation: {skipped}"
        )

    logger.info(
        f"Resolved {len(terms)} valid search terms "
        f"({sum(1 for t in terms if t in TICKER_NAME_OVERRIDES.values())} from overrides, "
        f"{sum(1 for t in terms if t not in TICKER_NAME_OVERRIDES.values())} raw symbols)"
    )
    return terms


# ── Query builder ─────────────────────────────────────────────────────────────
def build_queries(terms: list[str], chunk_size: int = QUERY_CHUNK_SIZE) -> list[str]:
    """
    Split terms into chunks and produce one NewsAPI OR-query per chunk.
    Keeps each query safely under NewsAPI's 500-char limit.
    """
    queries = []
    for i in range(0, len(terms), chunk_size):
        chunk = terms[i : i + chunk_size]
        queries.append(" OR ".join(f'"{t}"' for t in chunk))
    logger.info(f"Built {len(queries)} query chunks of up to {chunk_size} terms each")
    return queries


# ── Fetch ─────────────────────────────────────────────────────────────────────
def fetch_articles(hours: int = HOURS) -> list[dict]:
    terms = load_search_terms()
    queries = build_queries(terms)

    from_dt = datetime.now(timezone.utc) - timedelta(hours=hours)
    from_str = from_dt.strftime("%Y-%m-%dT%H:%M:%S")

    all_articles: list[dict] = []

    for idx, query in enumerate(queries, start=1):
        logger.info(f"Fetching chunk {idx}/{len(queries)}")
        response = newsapi.get_everything(
            q=query,
            language=LANGUAGE,
            sort_by=SORT_BY,
            from_param=from_str,
            page_size=PAGE_SIZE,
            page=1,
        )
        batch = response.get("articles", [])
        logger.info(f"  → {len(batch)} articles")
        all_articles.extend(batch)

    logger.info(f"Total articles fetched across all chunks: {len(all_articles)}")
    return all_articles


# ── Deduplicate (by URL, across all chunks) ───────────────────────────────────
def deduplicate(articles: list[dict]) -> list[dict]:
    seen, unique = set(), []
    for a in articles:
        url = (a.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        unique.append(a)
    logger.info(f"{len(unique)} unique articles after deduplication")
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
