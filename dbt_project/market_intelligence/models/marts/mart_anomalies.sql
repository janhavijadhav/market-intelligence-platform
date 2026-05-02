with metrics as (
    select * from {{ ref('stg_trade_metrics') }}
),

symbol_stats as (
    select
        symbol,
        round(avg(vwap), 4)    as mean_vwap,
        round(stddev(vwap), 4) as stddev_vwap
    from metrics
    group by symbol
),

anomalies as (
    select
        m.window_start,
        m.window_end,
        m.symbol,
        m.vwap,
        s.mean_vwap,
        s.stddev_vwap,
        round(abs(m.vwap - s.mean_vwap), 4) as deviation,
        case
            when s.stddev_vwap > 0
                and abs(m.vwap - s.mean_vwap) > 2 * s.stddev_vwap
            then true
            else false
        end as is_anomaly
    from metrics m
    join symbol_stats s on m.symbol = s.symbol
)

select * from anomalies
where is_anomaly = true
