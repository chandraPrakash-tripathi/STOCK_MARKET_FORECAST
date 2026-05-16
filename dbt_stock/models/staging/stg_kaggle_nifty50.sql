{{
    config(
        materialized = 'view',
        schema = 'staging'
    )
}}

with source as (
    select * from {{source('raw', 'kaggle_nifty50')}}
),
cleaned as (
    SELECT
        symbol                                       as symbol,
        series,
        trade_date,

-- price columns cast to float for downstream arithmetic
prev_close::float                            as prev_close,
        open::float                                  as open_price,
        high::float                                  as high_price,
        low::float                                   as low_price,
        close::float                                 as close_price,
        vwap::float                                  as vwap,

-- volume / liquidity

coalesce(volume, 0)::bigint                  as volume,
        -- turnover in raw Kaggle is paisa; convert to INR crore for readability
        (coalesce(turnover, 0) / 10000000000.0)      as turnover_cr,
        coalesce(trades, 0)::bigint                  as trades,
        coalesce(deliverable_vol, 0)::bigint         as deliverable_vol,
        coalesce(pct_deliverable, 0)::float          as pct_deliverable,

        ingested_at
    from source
    where
        close is not null
        and trade_date is not null
        and symbol is not null
        -- drop obvious data errors
        and close > 0
        and open > 0
)

select * from cleaned