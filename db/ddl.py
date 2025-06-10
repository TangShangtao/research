"""
cache建表语句
"""
from . import engine
from . import table

engine.FUTURE_DB_ORIGIN.command(
    f"""
    CREATE TABLE IF NOT EXISTS {table.FUTURE_TICK} (
        trading_day Date NOT NULL,
        symbol LowCardinality(String) NOT NULL,
        symbol_type LowCardinality(String) NOT NULL,
        exchange_symbol LowCardinality(String) NOT NULL,
        exchange LowCardinality(String) NOT NULL,
        tick_time DateTime64(6,'Asia/Shanghai') NOT NULL,
        last_price Float64 NOT NULL,
        volume UInt32 NOT NULL,
        bid_price1 Float64 NOT NULL,
        bid_volume1 UInt32 NOT NULL,
        ask_price1 Float64 NOT NULL,
        ask_volume1 UInt32 NOT NULL,
        average_price Float64 NOT NULL,
        turnover Float64 NOT NULL,
        open_interest Float64 NOT NULL
    )
    ENGINE = MergeTree
    ORDER BY trading_day
    PARTITION BY symbol_type;
    """
)
engine.FUTURE_DB_ORIGIN.command(
    f"""
    CREATE TABLE IF NOT EXISTS {table.FUTURE_1MIN} (
        trading_day Date NOT NULL,
        symbol LowCardinality(String) NOT NULL,
        symbol_type LowCardinality(String) NOT NULL,
        exchange_symbol LowCardinality(String) NOT NULL,
        exchange LowCardinality(String) NOT NULL,
        bar_time DateTime64(6,'Asia/Shanghai') NOT NULL,
        open Float64 NOT NULL,
        high Float64 NOT NULL,
        low Float64 NOT NULL,
        close Float64 NOT NULL,
        volume UInt32 NOT NULL,
        turnover Float64 NOT NULL,
        open_interest Float64 NOT NULL
    )
    ENGINE = MergeTree
    ORDER BY trading_day
    PARTITION BY symbol_type;
    """
)
engine.FUTURE_DB_ORIGIN.command(
    f"""
    CREATE TABLE IF NOT EXISTS {table.FUTURE_RB_TICK} (
        trading_day Date NOT NULL,
        symbol LowCardinality(String) NOT NULL,
        symbol_type LowCardinality(String) NOT NULL,
        exchange_symbol LowCardinality(String) NOT NULL,
        exchange LowCardinality(String) NOT NULL,
        tick_time DateTime64(6,'Asia/Shanghai') NOT NULL,
        last_price Float64 NOT NULL,
        volume UInt32 NOT NULL,
        bid_price1 Float64 NOT NULL,
        bid_volume1 UInt32 NOT NULL,
        ask_price1 Float64 NOT NULL,
        ask_volume1 UInt32 NOT NULL,
        average_price Float64 NOT NULL,
        turnover Float64 NOT NULL,
        open_interest Float64 NOT NULL
    )
    ENGINE = MergeTree
    ORDER BY (trading_day, symbol)
    PARTITION BY toYear(trading_day);
    """
)
engine.FUTURE_DB_ORIGIN.command(
    f"""
    CREATE TABLE IF NOT EXISTS {table.FUTURE_1MIN} (
        trading_day Date NOT NULL,
        symbol LowCardinality(String) NOT NULL,
        symbol_type LowCardinality(String) NOT NULL,
        exchange_symbol LowCardinality(String) NOT NULL,
        exchange LowCardinality(String) NOT NULL,
        bar_time DateTime64(6,'Asia/Shanghai') NOT NULL,
        open Float64 NOT NULL,
        high Float64 NOT NULL,
        low Float64 NOT NULL,
        close Float64 NOT NULL,
        volume UInt32 NOT NULL,
        turnover Float64 NOT NULL,
        open_interest Float64 NOT NULL
    )
    ENGINE = MergeTree
    ORDER BY trading_day
    PARTITION BY symbol_type;
    """
)