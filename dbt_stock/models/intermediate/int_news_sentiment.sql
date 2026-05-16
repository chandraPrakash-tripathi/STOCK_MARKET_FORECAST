-- models/intermediate/int_news_sentiment.sql
-- Aggregates raw articles per symbol per day.
-- Actual sentiment scores (from Ollama) are written back to raw.news_articles
-- by the Airflow sentiment DAG and picked up here.
-- Until then, article_count acts as a proxy signal.
{{
  config(
    materialized = 'table',
    schema       = 'intermediate'
  )
}}

with articles as (
    select * from {{ ref('stg_news_articles') }}
),

-- Map articles to Nifty symbols by keyword matching in title.
-- Replace with a proper symbol_lookup seed table once available.

symbol_mapped as (
    select
        a.*,
        s.symbol
    from articles a
    cross join (
        select distinct symbol from {{ ref('stg_kaggle_nifty50') }}
    ) s
    where lower(a.title) like '%' || lower(s.symbol) || '%'
       or lower(a.sentiment_text) like '%' || lower(s.symbol) || '%'
),

daily_agg as (
    select
        symbol,
        article_date                                  as sentiment_date,
        count(*)                                      as article_count,

-- Placeholder: replace avg_sentiment_score with Ollama scores
-- once the sentiment enrichment DAG populates raw.news_articles.sentiment_score
null::float                                   as avg_sentiment_score,
        null::float                                   as sentiment_std,
        null::int                                     as positive_count,
        null::int                                     as negative_count,
        null::int                                     as neutral_count,

-- Keep a sample headline for the LangGraph news agent
max(title)                                    as sample_headline

    from symbol_mapped
    group by 1, 2
)

select * from daily_agg