import  polars as pl
import datetime as dt
import typing
import db

__FUTURE_BASIC_INFO = pl.read_database(
    f"""
    SELECT
        *
    FROM {db.table.FUTURE_BASIC_INFO}
    """,
    db.engine.FUTURE_DB,
    schema_overrides={"night_trading":pl.Boolean}
)
__TRADING_DAY = pl.read_database(
    f"""
    SELECT
        *
    FROM {db.table.TRADING_DAYS}
    """,
    db.engine.FUTURE_DB,
    schema_overrides={"night_trading":pl.Boolean}
)

def trading_days() -> pl.DataFrame:
    return __TRADING_DAY

def pre_trading_day(
        trading_day: dt.date,
        gap: int = 1
) -> dt.date:
    """
    获取往前gap个交易日
    :param trading_day: 交易日
    :param gap: 间隔
    :return: 过去交易日
    """
    if gap == 0:
        return trading_day
    return (
        __TRADING_DAY
        .select("trading_day")
        .filter(pl.col("trading_day") < trading_day)
        .sort("trading_day", descending=True)["trading_day"][gap - 1]
    )

def symbol_type_info(
        start_trading_day: dt.date,
        end_trading_day: dt.date,
        fields: list[str]
) -> pl.DataFrame:
    """
    开始到结束期间期货品种基础字段
    :param start_trading_day: 开始交易日
    :param end_trading_day: 结束交易日
    :param fields: 字段
    :return: 开始到结束期间期货品种基础字段
    """
    return pl.concat([
        (
            __FUTURE_BASIC_INFO
            .filter(
                (pl.col("trading_day") > start_trading_day) &
                (pl.col("trading_day") <= end_trading_day)
            )
        ),
        (
            __FUTURE_BASIC_INFO
            .filter(pl.col("trading_day") <= start_trading_day)
            .filter(
                pl.col("trading_day") == pl.col("trading_day").max().over("symbol_type")
            )
            .with_columns(pl.lit(start_trading_day).alias("trading_day"))
        )
    ]).select(fields)