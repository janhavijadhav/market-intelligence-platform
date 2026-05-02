import json
import time
import logging
import random
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_TRADES = "market.trades"
TOPIC_QUOTES = "market.quotes"

# Realistic base prices per symbol
BASE_PRICES = {
    "AAPL":  182.0,
    "GOOGL": 141.0,
    "MSFT":  415.0,
    "AMZN":  185.0,
    "TSLA":  245.0,
}

EXCHANGES = ["NASDAQ", "NYSE", "BATS", "EDGX"]


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8")
    )


def simulate_trade(symbol: str, base_price: float) -> dict:
    price = round(base_price + random.uniform(-2.0, 2.0), 2)
    BASE_PRICES[symbol] = price

    return {
        "symbol": symbol,
        "price": price,
        "size": random.randint(1, 500),
        "exchange": random.choice(EXCHANGES),
        "timestamp": datetime.utcnow().isoformat(),
        "ingested_at": datetime.utcnow().isoformat()
    }


def simulate_quote(symbol: str, base_price: float) -> dict:
    spread = round(random.uniform(0.01, 0.10), 2)
    mid    = round(base_price + random.uniform(-1.0, 1.0), 2)

    return {
        "symbol": symbol,
        "bid_price": round(mid - spread / 2, 2),
        "ask_price": round(mid + spread / 2, 2),
        "bid_size": random.randint(1, 200),
        "ask_size": random.randint(1, 200),
        "timestamp": datetime.utcnow().isoformat(),
        "ingested_at": datetime.utcnow().isoformat()
    }


def produce(producer: KafkaProducer, topic: str, records: list):
    for record in records:
        producer.send(
            topic,
            key=record["symbol"],
            value=record
        )
    producer.flush()
    logger.info(f"Produced {len(records)} records to [{topic}]")


def main():
    logger.info("Starting simulated market data producer...")
    producer = create_producer()

    while True:
        try:
            trades = [simulate_trade(s, p) for s, p in BASE_PRICES.items()]
            produce(producer, TOPIC_TRADES, trades)

            quotes = [simulate_quote(s, p) for s, p in BASE_PRICES.items()]
            produce(producer, TOPIC_QUOTES, quotes)

        except Exception as e:
            logger.error(f"Producer error: {e}")

        time.sleep(10)


if __name__ == "__main__":
    main()
