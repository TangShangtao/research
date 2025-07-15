"""
原始数据落库
"""

import db
import polars as pl
from public import future_basic_info
import os
import zipfile
import tqdm
import re
import loguru
import sys

def zip_baiduyun_tick_file(
        path: str= "E:\\BaiduNetdiskDownload",
        year: str = "2011",
) -> None:
    """
    解压缩百度云tick文件
    :param path:
    :return:
    """
    [
        zipfile.ZipFile(os.path.join(path, year, file)).extractall(os.path.join(path, year, file.split(".")[0]))
        for file in tqdm.tqdm(os.listdir(os.path.join(path, year)))
    ]
    [
        os.remove(os.path.join(path, year, file))
        for file in os.listdir(os.path.join(path, year)) if file.endswith(".zip")
    ]
def future_tick_csv_to_ck(
        path: str= "E:\\BaiduNetdiskDownload",
        start_year: int = 2011,
        end_year: int = 2021,

) -> None:
    """
    百度云期货tick csv数据入库, 只落库rb
    :param path: 百度云下载文件路径
    :param start_year: 开始年
    :param end_year: 结束年
    :return:
    """
    min_trading_day_str = pl.read_database(
        f"""
        SELECT MIN(trading_day) AS min_trading_day
        FROM {db.table.FUTURE_TICK}
        """,
        db.engine.FUTURE_DB
    ).item(0,0).strftime("%Y%m%d")
    trading_days = []
    for year in range(start_year, end_year + 1):
        trading_days.extend([trading_day_str for trading_day_str in os.listdir(os.path.join(path, str(year))) if trading_day_str < min_trading_day_str])
    trading_days = sorted(trading_days, reverse=True)
    csv_detail = []
    for trading_day_str in trading_days:
        year = trading_day_str[:4]
        daily_csvs = os.listdir(os.path.join(path, year, trading_day_str))
        for daily_csv in daily_csvs:
            symbol_type = re.sub(r'\d+',"", daily_csv.split(".")[0])
            sub_month = re.findall(r"\d+", daily_csv)[0]
            if len(sub_month) == 3:
                sub_month = year[-2] + sub_month
            csv_detail.append({
                "symbol_type": symbol_type,
                "symbol": symbol_type + sub_month,
                "abs_path": os.path.join(path, year, trading_day_str, daily_csv),
            })
    csv_detail = pl.DataFrame(csv_detail)

    for symbol_type, symbol, abs_path in tqdm.tqdm(csv_detail.iter_rows(), total=csv_detail.shape[0]):
        try:
            csv_df = pl.read_csv(abs_path)
            # 考虑夜盘时间, 没有考虑郑商所合约名称
            csv_df_clean = (
                csv_df
                .with_columns(
                    pl.when(pl.col("UpdateTime") >= "16:00:00")
                    .then(
                        (pl.col("TradingDay").cast(pl.String).str.to_datetime("%Y%m%d") - pl.duration(days=1)).dt.strftime("%Y%m%d") +
                        " " +
                        pl.col("UpdateTime") +
                        "." +
                        pl.col("UpdateMillisec").cast(pl.String)
                    )
                    .otherwise(
                        pl.col("TradingDay").cast(pl.String) +
                         " " +
                         pl.col("UpdateTime") +
                         "." +
                         pl.col("UpdateMillisec").cast(pl.String)
                    )
                    .str.strptime(pl.Datetime(time_unit="us", time_zone="Asia/Shanghai"), "%Y%m%d %H:%M:%S%.f")
                    .alias("tick_time"),

                    pl.col("TradingDay").cast(pl.String).str.strptime(pl.Date, "%Y%m%d"),
                    pl.lit(symbol_type).alias("symbol_type"),
                    pl.lit(symbol).alias("symbol"),
                    pl.col("InstrumentID").alias("exchange_symbol"),
                    pl.lit("SHFE").alias("exchange")
                )
                .rename({
                    "TradingDay": "trading_day",
                    "LastPrice": "last_price",
                    "Volume": "volume",
                    "BidPrice1": "bid_price1",
                    "BidVolume1": "bid_volume1",
                    "AskPrice1": "ask_price1",
                    "AskVolume1": "ask_volume1",
                    "AveragePrice": "average_price",
                    "Turnover": "turnover",
                    "OpenInterest": "open_interest"
                })
                .select([
                    "trading_day", "symbol", "symbol_type", "exchange_symbol", "exchange",
                    "tick_time", "last_price", "volume",
                    "bid_price1", "bid_volume1", "ask_price1", "ask_volume1",
                    "average_price", "turnover", "open_interest"
                ])
                .with_columns(
                    pl.col("last_price").fill_null(0).alias("last_price"),
                    pl.col("average_price").fill_null(0).alias("average_price"),
                    pl.col("bid_price1").fill_null(0).alias("bid_price1"),
                    pl.col("ask_price1").fill_null(0).alias("ask_price1"),
                )
            )

            db.engine.FUTURE_DB_ORIGIN.insert_df(db.table.FUTURE_TICK, csv_df_clean.to_pandas())
        except Exception as e:
            loguru.logger.exception(e)
            loguru.logger.exception(abs_path)
            sys.exit(1)





def future_tick_to_future_main_tick(
        year: int = 2021
):
    """
    future_tick表中数据写入主力tick
    取昨日累计turnover最大的合约为主力
    :return:
    """
    trading_days = (
        pl.read_database(
            f"""
            SELECT distinct trading_day
            FROM {db.table.FUTURE_TICK}
            WHERE toYear(trading_day) = '{year}'
            AND trading_day >= '2021-06-17'
            ORDER BY trading_day ASC
            """,
            db.engine.FUTURE_DB
        )
    ).with_columns(pl.col("trading_day").shift(1).fill_null(strategy="backward").alias("pre_trading_day"))

    for trading_day, pre_trading_day in tqdm.tqdm(trading_days.iter_rows()):
        main_symbol_tick = (
            pl.read_database(
        f"""
                SELECT * 
                FROM {db.table.FUTURE_TICK}
                WHERE trading_day = '{pre_trading_day}' 
                AND (symbol_type, symbol) IN (
                    SELECT symbol_type, argMax(symbol, turnover) AS symbol
                    FROM {db.table.FUTURE_TICK}
                    WHERE trading_day = '{pre_trading_day}'
                    GROUP BY symbol_type
                    ORDER BY symbol_type
                )
                """,
                db.engine.FUTURE_DB,
            )
        )
        db.engine.FUTURE_DB_ORIGIN.insert_df(db.table.FUTURE_MAIN_TICK, main_symbol_tick.to_pandas())
