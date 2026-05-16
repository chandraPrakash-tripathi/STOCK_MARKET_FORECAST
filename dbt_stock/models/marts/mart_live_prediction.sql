-- models/marts/mart_live_prediction.sql
-- Inference-ready feature row per symbol.
-- Priority: if today's yfinance daily bar exists, use it.
-- Otherwise fall back to the most recent kaggle row.
-- Also exposes today's 5-min bars for the intraday LSTM agent.
{{
  config(
    materialized = 'table',
    schema       = 'marts'
  )
}}

with kaggle_signals as (
    select * from {{ ref('int_combined_signals') }}
),

-- most recent historical row per symbol from kaggle pipeline
kaggle_latest as (
    select distinct
        on (symbol) symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        vwap,
        volume,
        log_return,
        daily_return,
        rsi_14,
        macd_line,
        macd_signal,
        macd_histogram,
        ema_20,
        ema_50,
        ema_200,
        bb_pct_b,
        price_above_ema20,
        price_above_ema50,
        price_above_ema200,
        golden_cross_20_50,
        news_article_count,
        avg_sentiment_score,
        sample_headline,
        'kaggle' as data_source
    from kaggle_signals
    order by symbol, trade_date desc
),

-- today's aggregated daily bar from yfinance (may not exist yet today)
yf_today as (
    select
        symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        vwap,
        volume,
        daily_return,
        null::float                 as log_return,
        null::float                 as rsi_14,
        null::float                 as macd_line,
        null::float                 as macd_signal,
        null::float                 as macd_histogram,
        null::float                 as ema_20,
        null::float                 as ema_50,
        null::float                 as ema_200,
        null::float                 as bb_pct_b,
        null::int                   as price_above_ema20,
        null::int                   as price_above_ema50,
        null::int                   as price_above_ema200,
        null::int                   as golden_cross_20_50,
        0                           as news_article_count,
        null::float                 as avg_sentiment_score,
        null::text                  as sample_headline,
        'yfinance_live'             as data_source
    from {{ ref('stg_yfinance_daily') }}
    where trade_date = current_date
),

-- prefer yfinance today over kaggle historical
-- coalesce: take yf_today if available, else kaggle_latest
combined as (
    select
        coalesce(yf.symbol, k.symbol)                   as symbol,
        coalesce(yf.trade_date, k.trade_date)           as as_of_date,

-- price: live if available
coalesce(yf.open_price, k.open_price) as open_price,
coalesce(yf.high_price, k.high_price) as high_price,
coalesce(yf.low_price, k.low_price) as low_price,
coalesce(yf.close_price, k.close_price) as close_price,
coalesce(yf.vwap, k.vwap) as vwap,
coalesce(yf.volume, k.volume) as volume,

-- returns: yf daily return if live, else kaggle
coalesce(
    yf.daily_return,
    k.daily_return
) as daily_return,
k.log_return,

-- technicals: always from kaggle pipeline (dbt computes them)
-- yfinance nulls here get filled by next dbt run after close
coalesce(yf.rsi_14, k.rsi_14) as rsi_14,
coalesce(yf.macd_line, k.macd_line) as macd_line,
coalesce(yf.macd_signal, k.macd_signal) as macd_signal,
coalesce(
    yf.macd_histogram,
    k.macd_histogram
) as macd_histogram,
coalesce(yf.ema_20, k.ema_20) as ema_20,
coalesce(yf.ema_50, k.ema_50) as ema_50,
coalesce(yf.ema_200, k.ema_200) as ema_200,
coalesce(yf.bb_pct_b, k.bb_pct_b) as bb_pct_b,
coalesce(
    yf.price_above_ema20,
    k.price_above_ema20
) as price_above_ema20,
coalesce(
    yf.price_above_ema50,
    k.price_above_ema50
) as price_above_ema50,
coalesce(
    yf.price_above_ema200,
    k.price_above_ema200
) as price_above_ema200,
coalesce(
    yf.golden_cross_20_50,
    k.golden_cross_20_50
) as golden_cross_20_50,

-- sentiment: always from news pipeline
k.news_article_count,
k.avg_sentiment_score,
k.sample_headline,

-- audit column: tells the LSTM agent where this row came from
coalesce(yf.data_source, k.data_source)         as data_source

    from kaggle_latest k
    left join yf_today yf using (symbol)
)

select * from combined order by symbol