-- models/marts/mart_news_sentiment.sql
-- Sentiment feed for the LangGraph News Agent and Streamlit dashboard.
-- One row per symbol per day with all sentiment signals.
{{
  config(
    materialized = 'table',
    schema       = 'marts'
  )
}}

with sentiment as (
    select * from {{ ref('int_news_sentiment') }}
),

-- Attach latest close for context-aware agent prompts
latest_price as (
    select
        symbol,
        trade_date,
        close_price,
        daily_return
    from {{ ref('int_price_features') }}
)

select s.symbol, s.sentiment_date, s.article_count, s.avg_sentiment_score, s.sentiment_std, s.positive_count, s.negative_count, s.neutral_count, s.sample_headline,

-- price context on sentiment date
p.close_price,
p.daily_return
from
    sentiment s
    left join latest_price p on s.symbol = p.symbol
    and s.sentiment_date = p.trade_date
order by s.symbol, s.sentiment_date desc