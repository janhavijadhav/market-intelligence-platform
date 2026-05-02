with metrics as (
    select * from {{ ref('stg_trade_metrics') }}
),

performance as (
    select
        symbol,
        date_trunc('hour', window_start)        as hour,
        round(avg(vwap), 4)                     as avg_vwap,
        round(avg(avg_price), 4)                as avg_price,
        round(min(min_price), 4)                as session_low,
        round(max(max_price), 4)                as session_high,
        sum(total_volume)                       as total_volume,
        sum(trade_count)                        as total_trades,
        round(avg(price_volatility_pct), 4)     as avg_volatility_pct,
        round(max(max_price) - min(min_price), 4) as price_range
    from metrics
    group by symbol, date_trunc('hour', window_start)
)

select * from performance
