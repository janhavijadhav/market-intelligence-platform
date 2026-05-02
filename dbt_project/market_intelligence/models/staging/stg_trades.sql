with source as (
    select * from {{ source('raw', 'trades') }}
),

cleaned as (
    select
        symbol,
        price,
        size,
        exchange,
        timestamp,
        ingested_at,
        loaded_at,
        price * size as trade_value
    from source
    where
        price > 0
        and size > 0
        and symbol is not null
)

select * from cleaned
