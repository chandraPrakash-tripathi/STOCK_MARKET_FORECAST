-- models/intermediate/int_price_features.sql
-- Daily OHLCV enriched with return and gap features.
-- Source: int_price_source (kaggle history + yfinance live, unified).
-- All window functions are BACKWARD-ONLY to prevent data leakage.
{{
  config(
    materialized = 'table',
    schema       = 'intermediate'
  )
}}


with base as (
    -- ← only change from previous version
    select * from {{ ref('int_price_source') }}
),

with_returns as (
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
        trades,
        deliverable_vol,
        pct_deliverable,
        price_source,       -- carry through for audit / mart_live_prediction

-- simple daily return
case
            when lag(close_price) over (
                partition by symbol order by trade_date
            ) > 0
            then round(
                ((close_price - lag(close_price) over (
                    partition by symbol order by trade_date
                )) / lag(close_price) over (
                    partition by symbol order by trade_date
                ))::numeric, 6)
            else null
        end                                                      as daily_return,

-- overnight gap (open vs prev close)
case
            when lag(close_price) over (
                partition by symbol order by trade_date
            ) > 0
            then round(
                ((open_price - lag(close_price) over (
                    partition by symbol order by trade_date
                )) / lag(close_price) over (
                    partition by symbol order by trade_date
                ))::numeric, 6)
            else null
        end                                                      as overnight_gap,

-- intraday range as fraction of open
round(
            ((high_price - low_price) / nullif(open_price, 0))::numeric, 6
        )                                                        as intraday_range_pct,

-- log return
case
            when lag(close_price) over (
                partition by symbol order by trade_date
            ) > 0
            then round(
                ln(close_price / lag(close_price) over (
                    partition by symbol order by trade_date
                ))::numeric, 8)
            else null
        end                                                      as log_return,

-- delivery ratio (null for yfinance rows — no delivery data)
round(
            (deliverable_vol::float / nullif(volume, 0))::numeric, 6
        )                                                        as delivery_ratio,

-- rolling volume z-score (backward 20-day window)
case
            when count(volume) over (
                partition by symbol
                order by trade_date
                rows between 19 preceding and 1 preceding
            ) >= 10
            then round((
                (volume - avg(volume) over (
                    partition by symbol
                    order by trade_date
                    rows between 19 preceding and 1 preceding
                )) / nullif(stddev(volume) over (
                    partition by symbol
                    order by trade_date
                    rows between 19 preceding and 1 preceding
                ), 0)
            )::numeric, 4)
            else null
        end                                                      as volume_zscore_20d

    from base
)

select * from with_returns