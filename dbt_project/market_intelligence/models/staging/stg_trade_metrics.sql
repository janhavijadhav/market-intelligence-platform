with source as (
    select * from {{ source('raw', 'trade_metrics') }}
),

cleaned as (
    select
        window_start,
        window_end,
        symbol,
        vwap,
        avg_price,
        min_price,
        max_price,
        total_volume,
        trade_count,
        processed_at,
        max_price - min_price as price_range,
        case
            when avg_price > 0
            then round((max_price - min_price) / avg_price * 100, 4)
            else null
        end as price_volatility_pct
    from source
    where symbol is not null
)

select * from cleaned
