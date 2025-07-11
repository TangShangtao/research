import db
import polars as pl
import datetime as dt
from public import future_basic_info
import typing

def read_main_min1(
        start_trading_day: dt.date,
        end_trading_day: dt.date
) -> pl.DataFrame:
    """
    读取期货主力1分钟bar
    :param start_trading_day: 开始日期
    :param end_trading_day: 结束日期
    :return: 期货主力1分钟bar
    """
    return pl.read_database(
        f"""
        SELECT
            *
        FROM {db.table.FUTURE_MAIN_MIN1}
        WHERE 
            trading_day >= '{start_trading_day}'
        AND 
            trading_day <= '{end_trading_day}'
        ORDER BY trading_day ASC, bar_time ASC
        """,
        db.engine.FUTURE_DB
    )

def read_main_tick(
        start_trading_day: dt.date,
        end_trading_day: dt.date,
        symbol_type: str,
        every: typing.Literal["1mo","1d"] = "1mo"
) -> tuple[typing.Iterator[pl.DataFrame], int]:
    """
    读取时间区间内品种主力tick
    :param start_trading_day: 开始交易日
    :param end_trading_day: 结束交易日
    :param symbol_type: 品种
    :param every: 按时间划分batch
    :return: tick数据迭代器
    """
    batch = (
        future_basic_info.trading_days()
        .filter(
            (pl.col("trading_day") >= start_trading_day) &
            (pl.col("trading_day") <= end_trading_day)
        )
        .select("trading_day")
        .group_by_dynamic(
            "trading_day",
            every=every,
            closed="left",
            label="left"
        )
        .agg(
            pl.col("trading_day").min().alias("start"),
            pl.col("trading_day").max().alias("end")
        )
    )
    def _iter():
        for _, start, end in batch.iter_rows():
            yield pl.read_database(
                f"""
                SELECT
                    *
                FROM {db.table.FUTURE_MAIN_TICK}
                WHERE 
                    trading_day >= '{start}'
                AND 
                    trading_day <= '{end}'
                AND symbol_type = '{symbol_type}'
                ORDER BY trading_day ASC, tick_time ASC
                """,
                db.engine.FUTURE_DB
            )
    return _iter(), batch.shape[0]

def read_tick(
        start_trading_day: dt.date,
        end_trading_day: dt.date,
        symbol_type: str,
        every: typing.Literal["1mo","1d"] = "1mo"
) -> tuple[typing.Iterator[pl.DataFrame], int]:
    """
    读取时间区间内品种tick
    :param start_trading_day: 开始交易日
    :param end_trading_day: 结束交易日
    :param symbol_type: 品种
    :param every: 按时间划分batch
    :return: tick数据迭代器
    """
    batch = (
        future_basic_info.trading_days()
        .filter(
            (pl.col("trading_day") >= start_trading_day) &
            (pl.col("trading_day") <= end_trading_day)
        )
        .select("trading_day")
        .group_by_dynamic(
            "trading_day",
            every=every,
            closed="left",
            label="left"
        )
        .agg(
            pl.col("trading_day").min().alias("start"),
            pl.col("trading_day").max().alias("end")
        )
    )
    def _iter():
        for _, start, end in batch.iter_rows():
            yield pl.read_database(
                f"""
                SELECT
                    *
                FROM {db.table.FUTURE_TICK}
                WHERE 
                    trading_day >= '{start}'
                AND 
                    trading_day <= '{end}'
                AND symbol_type = '{symbol_type}'
                ORDER BY trading_day ASC, tick_time ASC
                """,
                db.engine.FUTURE_DB
            )
    return _iter(), batch.shape[0]