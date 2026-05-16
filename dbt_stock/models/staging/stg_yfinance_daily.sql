-- models/staging/stg_yfinance_daily.sql
-- Aggregates 5-min bars to daily OHLCV.
-- Grain: one row per symbol per trade_date.
-- Used to extend kaggle history with live data in int_price_features.
{{
  config(
    materialized = 'view',
    schema       = 'staging'
  )
}}


with bars as (
    select * from {{ ref('stg_yfinance_ohlcv') }}
),

daily as (
    select
        symbol,
        trade_date,

-- OHLCV aggregation
-- open = first bar's open, close = last bar's close
min(
    case
        when bar_seq_asc = 1 then open_price
    end
) as open_price,
max(high_price) as high_price,
min(low_price) as low_price,
min(
    case
        when bar_seq_desc = 1 then close_price
    end
) as close_price,
sum(volume) as volume,

-- bar count as data quality signal (full session = ~75 bars)
count(*) as bar_count,

-- session VWAP: sum(price * vol) / sum(vol)
round(
            (sum(close_price * volume) / nullif(sum(volume), 0))::numeric,
        4)                                                      as vwap,

        max(ingested_at)                                        as ingested_at

    from bars
    group by symbol, trade_date
),

-- daily return vs previous session close

with_return as (
    select
        *,
        lag(close_price) over (
            partition by symbol
            order by trade_date
        )                                                       as prev_close,

        round(
            ((close_price - lag(close_price) over (
                partition by symbol order by trade_date
            )) / nullif(lag(close_price) over (
                partition by symbol order by trade_date
            ), 0))::numeric,
        6)                                                      as daily_return

    from daily
)

select * from with_return