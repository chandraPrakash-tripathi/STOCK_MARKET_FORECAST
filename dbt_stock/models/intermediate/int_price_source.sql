-- models/intermediate/int_price_source.sql
-- Unified daily price source merging kaggle historical data and yfinance live bars.
-- Grain: one row per (symbol, trade_date).
-- Priority rule: if both sources have a row for the same (symbol, trade_date),
-- yfinance wins — it is fresher and intraday-accurate.
-- All downstream models (int_price_features, int_technical_indicators, marts)
-- ref this model instead of stg_kaggle_nifty50 directly.
{{
  config(
    materialized = 'table',
    schema       = 'intermediate'
  )
}}


with kaggle as (
    select
        symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        vwap,
        volume,
        turnover_cr,
        pct_deliverable,
        -- kaggle carries these; yfinance does not
        deliverable_vol,
        trades,
        'kaggle'            as price_source
    from {{ ref('stg_kaggle_nifty50') }}
),

yfinance as (
    select
        symbol,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        vwap,
        volume,
        null::numeric       as turnover_cr,
        null::numeric       as pct_deliverable,
        null::bigint        as deliverable_vol,
        null::bigint        as trades,
        'yfinance'          as price_source
    from {{ ref('stg_yfinance_daily') }}
),

-- combine both sources
unioned as (
    select *
    from kaggle
    union all
    select *
    from yfinance
),

-- deduplicate: for any (symbol, trade_date) that exists in both,
-- keep the yfinance row (row_number = 1 because 'yfinance' sorts before 'kaggle'
-- when ordered by price_source asc — flip to desc to prefer kaggle)
deduped as (
    select *, row_number() over (
            partition by
                symbol, trade_date
            order by
                case price_source
                    when 'yfinance' then 1 -- yfinance wins
                    when 'kaggle' then 2
                end
        ) as rn
    from unioned
)

select
    symbol,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    vwap,
    volume,
    turnover_cr,
    pct_deliverable,
    deliverable_vol,
    trades,
    price_source
from deduped
where
    rn = 1
order by symbol, trade_date