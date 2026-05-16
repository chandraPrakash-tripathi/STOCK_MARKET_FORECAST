-- models/marts/mart_lstm_training.sql
-- Final feature matrix consumed by the LSTM training pipeline.
-- Excludes the forward labels — those are in mart_trend_training.
-- Time-safe split columns allow Python to slice without re-sorting.
{{
  config(
    materialized = 'table',
    schema       = 'marts'
  )
}}

with signals as (
    select * from {{ ref('int_combined_signals') }}
),

-- Define deterministic train / val / test splits by date cutoff.
-- Adjust cutoffs as dataset grows.
split_tagged as (
    select
        *,
        case
            when trade_date < '2020-01-01' then 'train'
            when trade_date < '2022-01-01' then 'val'
            else 'test'
        end as split
    from signals
    where
        -- require at least 200 trading days of history for EMA-200 to warm up
        close_price is not null
        and ema_200 is not null
        and rsi_14 is not null
        -- drop the last row per symbol (next_close is null — no label to predict)
        and next_close is not null
)

select symbol, trade_date, split,

-- ── LSTM input features (all numeric, no leakage) ──────────────────
open_price,
high_price,
low_price,
close_price,
vwap,
volume,
log_return,
daily_return,
overnight_gap,
intraday_range_pct,
delivery_ratio,
volume_zscore_20d,
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
coalesce(news_article_count, 0) as news_article_count,

-- ── regression target ───────────────────────────────────────────────
next_close as target_close
from split_tagged
order by symbol, trade_date