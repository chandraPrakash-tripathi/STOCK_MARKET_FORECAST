-- models/intermediate/int_combined_signals.sql
-- Master feature table: price features + technical indicators + sentiment,
-- all aligned on (symbol, trade_date). This is the single source of truth
-- for both training marts and live prediction.
{{
  config(
    materialized = 'table',
    schema       = 'intermediate'
  )
}}


with prices as (
    select * from {{ ref('int_price_features') }}
),

technicals as (
    select * from {{ ref('int_technical_indicators') }}
),

sentiment as (
    select * from {{ ref('int_news_sentiment') }}
),

joined as (
    select
        p.symbol,
        p.trade_date,

-- ── raw price features ──────────────────────────────────────────
p.open_price,
p.high_price,
p.low_price,
p.close_price,
p.vwap,
p.volume,
p.turnover_cr,
p.pct_deliverable,
p.daily_return,
p.overnight_gap,
p.intraday_range_pct,
p.log_return,
p.delivery_ratio,
p.volume_zscore_20d,

-- ── technical indicators ────────────────────────────────────────
t.rsi_14,
t.macd_line,
t.macd_signal,
t.macd_histogram,
t.ema_20,
t.ema_50,
t.ema_200,
t.bb_middle,
t.bb_upper,
t.bb_lower,
t.bb_pct_b,

-- price vs EMA signals (above/below flags)
case
    when p.close_price > t.ema_20 then 1
    else 0
end as price_above_ema20,
case
    when p.close_price > t.ema_50 then 1
    else 0
end as price_above_ema50,
case
    when p.close_price > t.ema_200 then 1
    else 0
end as price_above_ema200,
case
    when t.ema_20 > t.ema_50 then 1
    else 0
end as golden_cross_20_50,

-- ── news sentiment ──────────────────────────────────────────────
coalesce(s.article_count, 0) as news_article_count,
s.avg_sentiment_score,
s.positive_count,
s.negative_count,
s.sample_headline,

-- ── forward label for supervised training (leakage-safe) ───────
-- next_close is EXCLUDED from feature columns; only present for labelling
lead(p.close_price) over (
            partition by p.symbol
            order by p.trade_date
        )                                                       as next_close,

        lead(p.daily_return) over (
            partition by p.symbol
            order by p.trade_date
        )                                                       as next_daily_return

    from prices p
    left join technicals t
        on p.symbol = t.symbol and p.trade_date = t.trade_date
    left join sentiment s
        on p.symbol = s.symbol and p.trade_date = s.sentiment_date
)

select * from joined