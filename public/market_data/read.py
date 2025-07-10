import db
import polars as pl
import datetime as dt
from public import future_basic_info
import typing

def read_future_main_min1(
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

def read_rb_main_tick(
        start_trading_day: dt.date,
        end_trading_day: dt.date,
        every: typing.Literal["1mo","1d"] = "1mo"
) -> typing.Iterator[pl.DataFrame]:
    """

    :param start_trading_day:
    :param end_trading_day:
    :param every:
    :return:
    """
    batch = (
        future_basic_info.__TRADING_DAY
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
    for _, start, end in batch.iter_rows():
        yield pl.read_database(
            f"""
            SELECT
                *
            FROM {db.table.FUTURE_RB_MAIN_TICK}
            WHERE 
                trading_day >= '{start}'
            AND 
                trading_day <= '{end}'
            ORDER BY trading_day ASC, tick_time ASC
            """,
            db.engine.FUTURE_DB
        )