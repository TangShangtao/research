"""
合成数据
"""
import db
import polars as pl
import datetime as dt
import tqdm

def future_main_tick_to_main_min1(
) -> None:
    """
    期货主力tick数据合成为主力1分钟数据
    :return:
    """
    trading_days = pl.read_database(
        f"""
        SELECT distinct trading_day
        FROM {db.table.FUTURE_RB_MAIN_TICK}
        ORDER BY trading_day ASC
        """,
        db.engine.FUTURE_DB
    )["trading_day"]
    for trading_day in tqdm.tqdm(trading_days):
        tick = pl.read_database(
            f"""
            SELECT * 
            FROM {db.table.FUTURE_RB_MAIN_TICK}
            WHERE trading_day = '{trading_day}'
            ORDER BY tick_time ASC
            """,
            db.engine.FUTURE_DB
        )
        min = (
            tick
            .group_by_dynamic("tick_time", every="1m")
            .agg(
                pl.col("trading_day").first(),
                pl.col("symbol").first(),
                pl.col("symbol_type").first(),
                pl.col("exchange_symbol").first(),
                pl.col("exchange").first(),
                pl.col("last_price").first().alias("open"),
                pl.col("last_price").max().alias("high"),
                pl.col("last_price").min().alias("low"),
                pl.col("last_price").last().alias("close"),
                pl.col("volume").last(),
                pl.col("turnover").last(),
                pl.col("open_interest").last().alias("open_interest"),
            )
            .sort("tick_time")
            .filter(pl.col("volume") != 0)
            .with_columns(
                pl.col("volume").diff().fill_null(pl.col("volume")),
                pl.col("turnover").diff().fill_null(pl.col("turnover")),
                pl.col("tick_time").dt.replace_time_zone("Asia/Shanghai")
            )
            .rename({"tick_time":"bar_time"})
        )
        db.engine.FUTURE_DB_ORIGIN.insert_df(db.table.FUTURE_MAIN_MIN1, min.to_pandas())

