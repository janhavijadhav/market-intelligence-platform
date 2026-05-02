import json
import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import redis
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Market Intelligence API",
    description="Real-time financial market analytics",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis client
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True
)

CACHE_TTL = 60  # seconds


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
    )


def query_snowflake(sql: str) -> list:
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [col[0].lower() for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return rows


def cached_query(cache_key: str, sql: str) -> list:
    """Check Redis cache first, fall back to Snowflake."""
    cached = redis_client.get(cache_key)
    if cached:
        logger.info(f"Cache hit: {cache_key}")
        return json.loads(cached)

    logger.info(f"Cache miss: {cache_key} — querying Snowflake")
    data = query_snowflake(sql)

    # serialize datetime objects to string
    serializable = json.loads(
        json.dumps(data, default=str)
    )
    redis_client.setex(cache_key, CACHE_TTL, json.dumps(serializable))
    return serializable


@app.get("/")
def root():
    return {"message": "Market Intelligence API is running"}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/symbols")
def get_symbols():
    """Get list of all tracked symbols."""
    cache_key = "symbols"
    sql = """
        SELECT DISTINCT symbol
        FROM market_intelligence.raw.trades
        ORDER BY symbol
    """
    return cached_query(cache_key, sql)


@app.get("/performance")
def get_performance():
    """Get hourly performance for all symbols."""
    cache_key = "performance:all"
    sql = """
        SELECT *
        FROM market_intelligence.marts.mart_symbol_performance
        ORDER BY hour DESC, symbol
        LIMIT 50
    """
    return cached_query(cache_key, sql)


@app.get("/performance/{symbol}")
def get_symbol_performance(symbol: str):
    """Get hourly performance for a specific symbol."""
    symbol = symbol.upper()
    cache_key = f"performance:{symbol}"
    sql = f"""
        SELECT *
        FROM market_intelligence.marts.mart_symbol_performance
        WHERE symbol = '{symbol}'
        ORDER BY hour DESC
        LIMIT 24
    """
    data = cached_query(cache_key, sql)
    if not data:
        raise HTTPException(status_code=404, detail=f"No data found for {symbol}")
    return data


@app.get("/anomalies")
def get_anomalies():
    """Get detected price anomalies."""
    cache_key = "anomalies:all"
    sql = """
        SELECT *
        FROM market_intelligence.marts.mart_anomalies
        ORDER BY window_start DESC
        LIMIT 50
    """
    return cached_query(cache_key, sql)


@app.get("/latest-trades")
def get_latest_trades():
    """Get the latest trade for each symbol."""
    cache_key = "latest:trades"
    sql = """
        SELECT DISTINCT
            symbol,
            LAST_VALUE(price) OVER (
                PARTITION BY symbol
                ORDER BY timestamp
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as latest_price,
            LAST_VALUE(timestamp) OVER (
                PARTITION BY symbol
                ORDER BY timestamp
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as latest_timestamp
        FROM market_intelligence.raw.trades
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) = 1
        ORDER BY symbol
    """
    return cached_query(cache_key, sql)