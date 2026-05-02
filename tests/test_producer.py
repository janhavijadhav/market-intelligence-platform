import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.market_producer import simulate_trade, simulate_quote, BASE_PRICES


def test_simulate_trade_returns_correct_keys():
    trade = simulate_trade("AAPL", 182.0)
    assert "symbol" in trade
    assert "price" in trade
    assert "size" in trade
    assert "exchange" in trade
    assert "timestamp" in trade
    assert "ingested_at" in trade


def test_simulate_trade_price_is_positive():
    trade = simulate_trade("AAPL", 182.0)
    assert trade["price"] > 0


def test_simulate_trade_size_is_positive():
    trade = simulate_trade("AAPL", 182.0)
    assert trade["size"] > 0


def test_simulate_trade_symbol_matches():
    trade = simulate_trade("MSFT", 415.0)
    assert trade["symbol"] == "MSFT"


def test_simulate_quote_returns_correct_keys():
    quote = simulate_quote("AAPL", 182.0)
    assert "symbol" in quote
    assert "bid_price" in quote
    assert "ask_price" in quote
    assert "bid_size" in quote
    assert "ask_size" in quote


def test_simulate_quote_ask_greater_than_bid():
    quote = simulate_quote("AAPL", 182.0)
    assert quote["ask_price"] > quote["bid_price"]


def test_all_symbols_have_base_prices():
    symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
    for symbol in symbols:
        assert symbol in BASE_PRICES
