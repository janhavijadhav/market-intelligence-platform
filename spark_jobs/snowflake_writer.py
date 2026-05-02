import snowflake.connector
from dotenv import load_dotenv
import os
import logging

load_dotenv()

logger = logging.getLogger(__name__)


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
    )


def write_metrics_to_snowflake(batch_df, batch_id):
    rows = batch_df.collect()

    if not rows:
        logger.info(f"Batch {batch_id} was empty, skipping.")
        return

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO market_intelligence.raw.trade_metrics (
            window_start, window_end, symbol,
            vwap, avg_price, min_price, max_price,
            total_volume, trade_count, processed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = [
        (
            row.window_start,
            row.window_end,
            row.symbol,
            row.vwap,
            row.avg_price,
            row.min_price,
            row.max_price,
            row.total_volume,
            row.trade_count,
            row.processed_at
        )
        for row in rows
    ]

    cursor.executemany(insert_sql, records)
    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Batch {batch_id}: wrote {len(records)} records to Snowflake")


def write_trades_to_snowflake(batch_df, batch_id):
    rows = batch_df.collect()

    if not rows:
        return

    conn = get_snowflake_connection()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO market_intelligence.raw.trades (
            symbol, price, size, exchange, timestamp, ingested_at
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """

    records = [
        (
            row.symbol,
            row.price,
            row.size,
            row.exchange,
            row.timestamp,
            row.ingested_at
        )
        for row in rows
    ]

    cursor.executemany(insert_sql, records)
    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Batch {batch_id}: wrote {len(records)} raw trades to Snowflake")
