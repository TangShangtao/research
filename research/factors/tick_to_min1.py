"""
tick数据生成min1信号
"""
import polars as pl
import db
import datetime as dt

def orderbook_imbalance(
        data: pl.DataFrame
) -> pl.DataFrame:
    """
    订单簿不平衡因子
    :return:
    """
    required_data = (
        data.select([
            "trading_day",
            "symbol",
            "symbol_type",
            "exchange_symbol",
            "exchange",
            "tick_time",
            "bid_volume1",
            "ask_volume1"
        ])
        .with_columns(
            pl.col("bid_volume1").cast(pl.Int32),
            pl.col("ask_volume1").cast(pl.Int32),
        )
    )
    factor_tick = (
        required_data
        .with_columns(
            (
                (pl.col("bid_volume1") - pl.col("ask_volume1")) /
                (pl.col("bid_volume1") + pl.col("ask_volume1"))
            ).fill_nan(0).alias("factor"),
        )
    )
    factor_min1 = (
        factor_tick.group_by_dynamic(
            "tick_time",
            every="1m"
        )
        .agg(
            pl.col("symbol").first(),
            pl.col("symbol_type").first(),
            pl.col("exchange_symbol").first(),
            pl.col("exchange").first(),
            pl.col("factor").sum()
        )
        .rename({"tick_time":"bar_time"})
        .with_columns(
            (
                (pl.col("factor") - pl.col("factor").rolling_mean(window_size=5)) / pl.col("factor").rolling_std(window_size=5)
            )
            .fill_null(0)
            .tanh()
        )
    )

    return factor_min1

