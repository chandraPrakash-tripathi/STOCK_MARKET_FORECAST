-- models/staging/stg_yfinance_ohlcv.sql
-- Cleans 5-min bars from raw.ohlcv_live.
-- Two outputs in one model:
--   1. stg_yfinance_ohlcv        → 5-min bars (for intraday mart)
--   2. stg_yfinance_daily (view) → OHLCV aggregated to daily (for joining kaggle features)
{{
  config(
    materialized = 'view',
    schema       = 'staging'
  )
}}


with source as (
    select * from {{ source('raw', 'ohlcv_live') }}
),

cleaned as (
    select
        -- strip .NS suffix to match kaggle_nifty50 symbol format
        replace(ticker, '.NS', '')              as symbol,
        ticker                                  as yf_ticker,

-- convert UTC → IST for human-readable times
timestamp at time zone 'Asia/Kolkata' as bar_time_ist,
timestamp as bar_time_utc,

-- date of bar in IST (for daily joins)
(timestamp at time zone 'Asia/Kolkata')::date
                                                as trade_date,

-- IST hour for market session filtering
extract(hour from timestamp at time zone 'Asia/Kolkata')::int
                                                as bar_hour_ist,

        open::float                             as open_price,
        high::float                             as high_price,
        low::float                              as low_price,
        close::float                            as close_price,
        coalesce(volume, 0)::bigint             as volume,
        ingested_at

    from source
    where
        close is not null
        and close > 0
        -- drop zero-volume opening auction bars (like your id=1 row)
        and coalesce(volume, 0) > 0
        -- only NSE market hours: 9:15 AM to 3:30 PM IST
        -- in UTC: 03:45 to 10:00
        and timestamp >= (current_date - interval '7 days')  -- rolling 7-day window
),

-- tag each bar's position in the session (useful for mart_live_prediction)
with_session_tags as (
    select
        *,
        -- first bar of the day per symbol
        row_number() over (
            partition by symbol, trade_date
            order by bar_time_utc asc
        )                                       as bar_seq_asc,

-- last bar of the day per symbol (for "latest close" logic)
row_number() over (
    partition by
        symbol,
        trade_date
    order by bar_time_utc desc
) as bar_seq_desc,

-- running volume for the session
sum(volume) over (
    partition by
        symbol,
        trade_date
    order by
        bar_time_utc rows between unbounded preceding
        and current row
) as cumulative_volume,

-- bar-level return
round(
            ((close_price - lag(close_price) over (
                partition by symbol, trade_date
                order by bar_time_utc
            )) / nullif(lag(close_price) over (
                partition by symbol, trade_date
                order by bar_time_utc
            ), 0))::numeric,
        6)                                      as bar_return

    from cleaned
)

select * from with_session_tags