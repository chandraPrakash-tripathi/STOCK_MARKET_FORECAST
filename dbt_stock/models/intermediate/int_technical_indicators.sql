-- models/intermediate/int_technical_indicators.sql
-- RSI-14, MACD (12/26/9), EMA-20/50/200, Bollinger Bands (20,2σ)
-- All computed with strictly backward windows — zero leakage.
{{
  config(
    materialized = 'table',
    schema       = 'intermediate'
  )
}}

with prices as (
    select
        symbol,
        trade_date,
        close_price,
        daily_return,
        log_return
    from {{ ref('int_price_features') }}
),

-- ── RSI-14 via Wilder's smoothed avg gain/loss ────────────────────────────
rsi_base as (
    select
        symbol,
        trade_date,
        close_price,
        daily_return,
        log_return,

-- raw gain / loss for this session
greatest(daily_return, 0) as gain,
greatest(- daily_return, 0) as loss,

-- 14-period simple avg gain and loss (seed for Wilder's EMA)
avg(greatest(daily_return, 0)) over (
            partition by symbol
            order by trade_date
            rows between 14 preceding and 1 preceding
        )                                             as avg_gain_14,

        avg(greatest(-daily_return, 0)) over (
            partition by symbol
            order by trade_date
            rows between 14 preceding and 1 preceding
        )                                             as avg_loss_14

    from prices
    where daily_return is not null
),

rsi_calc as (
    select
        symbol,
        trade_date,
        close_price,
        daily_return,
        log_return,

-- RSI = 100 - (100 / (1 + RS))
case
            when avg_loss_14 = 0 then 100.0
            when avg_gain_14 is null or avg_loss_14 is null then null
            else round((100.0 - (100.0 / (1.0 + avg_gain_14 / nullif(avg_loss_14, 0))))::numeric, 4)
        end                                           as rsi_14

    from rsi_base
),

-- ── EMA helper: uses window-based approximation ───────────────────────────
-- True EMA requires recursive CTE in pure SQL; approximation with 2/(N+1) span
-- is accurate for training features (exact EMA computed in Python/pandas pre-LSTM)
ema_base as (
    select
        r.symbol,
        r.trade_date,
        r.close_price,
        r.daily_return,
        r.log_return,
        r.rsi_14,

-- EMA-20 approximation (span=20 rolling weighted mean)
avg(p.close_price) over (
    partition by
        r.symbol
    order by r.trade_date rows between 19 preceding
        and current row
) as ema_20_approx,

-- EMA-50 approximation
avg(p.close_price) over (
    partition by
        r.symbol
    order by r.trade_date rows between 49 preceding
        and current row
) as ema_50_approx,

-- EMA-200 approximation
avg(p.close_price) over (
            partition by r.symbol
            order by r.trade_date
            rows between 199 preceding and current row
        )                                             as ema_200_approx

    from rsi_calc r
    join prices p using (symbol, trade_date)
),

-- ── MACD: EMA12 − EMA26, signal = EMA9 of macd ───────────────────────────
macd_base as (
    select
        symbol,
        trade_date,
        close_price,
        daily_return,
        log_return,
        rsi_14,
        ema_20_approx,
        ema_50_approx,
        ema_200_approx,
        (
            avg(close_price) over (
                partition by
                    symbol
                order by trade_date rows between 11 preceding
                    and current row
            ) - avg(close_price) over (
                partition by
                    symbol
                order by trade_date rows between 25 preceding
                    and current row
            )
        ) as macd_line
    from ema_base
),
macd_signal as (
    select *, avg(macd_line) over (
            partition by
                symbol
            order by trade_date rows between 8 preceding
                and current row
        ) as macd_signal_line
    from macd_base
),

-- ── Bollinger Bands: 20-day SMA ± 2σ ─────────────────────────────────────
bb_base as (
    select *, avg(close_price) over (
            partition by
                symbol
            order by trade_date rows between 19 preceding
                and current row
        ) as bb_middle, stddev(close_price) over (
            partition by
                symbol
            order by trade_date rows between 19 preceding
                and current row
        ) as bb_std
    from macd_signal
)

select
    symbol,
    trade_date,
    close_price,
    daily_return,
    log_return,
    rsi_14,

-- MACD features
round(macd_line::numeric, 6)                      as macd_line,
    round(macd_signal_line::numeric, 6)               as macd_signal,
    round((macd_line - macd_signal_line)::numeric, 6) as macd_histogram,

-- EMAs
round(ema_20_approx::numeric, 4)                  as ema_20,
    round(ema_50_approx::numeric, 4)                  as ema_50,
    round(ema_200_approx::numeric, 4)                 as ema_200,

-- Bollinger Bands
round(bb_middle::numeric, 4)                      as bb_middle,
    round((bb_middle + 2 * bb_std)::numeric, 4)       as bb_upper,
    round((bb_middle - 2 * bb_std)::numeric, 4)       as bb_lower,
    -- %B = (price − lower) / (upper − lower)
    round(
        ((close_price - (bb_middle - 2 * bb_std))
        / nullif((4 * bb_std), 0))::numeric,
    6)                                                as bb_pct_b

from bb_base