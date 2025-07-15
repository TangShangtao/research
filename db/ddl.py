"""
cache建表语句
"""
import db.table
from . import engine
from . import table
# 期货基本信息
engine.FUTURE_DB_ORIGIN.command(
    f"""
    CREATE TABLE IF NOT EXISTS {table.FUTURE_BASIC_INFO} (
        symbol_type LowCardinality(String) NOT NULL,
        product_class LowCardinality(String) NOT NULL,
        exchange LowCardinality(String) NOT NULL,
        multiplier Float64 NOT NULL,
        price_tick Float64 NOT NULL,
        night_trading UInt8 NOT NULL,
        trading_day Date NOT NULL,
        trading_hours String NOT NULL,
        close_ratio_by_money Float64 NOT NULL,
        close_ratio_by_volume Float64 NOT NULL,
        close_today_ratio_by_money Float64 NOT NULL,
        close_today_ratio_by_volume Float64 NOT NULL,
        open_ratio_by_money Float64 NOT NULL,
        open_ratio_by_volume Float64 NOT NULL,
        margin_rate Float64 NOT NULL
    )
    ENGINE = MergeTree
    ORDER BY symbol_type
    """
)
# 交易日
engine.FUTURE_DB_ORIGIN.command(
    f"""
    CREATE TABLE IF NOT EXISTS {table.TRADING_DAYS} (
        trading_day Date NOT NULL,
        night_trading UInt8 NOT NULL,
    )
    ENGINE = MergeTree
    ORDER BY trading_day
"""
)

# 期货全品种全合约tick
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
    PARTITION BY toYYYYMM(trading_day)
    ORDER BY (symbol_type, tick_time);
    """
)
# 期货全品种主力tick
engine.FUTURE_DB_ORIGIN.command(
    f"""
    CREATE TABLE IF NOT EXISTS {table.FUTURE_MAIN_TICK} (
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
    PARTITION BY toYYYYMM(trading_day)
    ORDER BY (symbol_type, tick_time);
    """
)
# 期货全品种主力1分钟
engine.FUTURE_DB_ORIGIN.command(
    f"""
    CREATE TABLE IF NOT EXISTS {table.FUTURE_MAIN_MIN1} (
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
        volume Int64 NOT NULL,
        turnover Float64 NOT NULL,
        open_interest Float64 NOT NULL
    )
    ENGINE = MergeTree
    PARTITION BY toYear(trading_day)
    ORDER BY (symbol_type, bar_time);
    """
)