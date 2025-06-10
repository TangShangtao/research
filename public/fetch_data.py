"""
原始数据落库
"""

# from xtquant import xtdata
import db
import polars as pl
from typing import Sequence
import datetime as dt
import os
import zipfile
import tqdm

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
        for file in os.listdir(os.path.join(path, year))
    ]
def future_tick_csv_to_ck(
        path: str= "E:\\BaiduNetdiskDownload",
        year: str = "2011",
        symbol_type: str="rb"
) -> None:
    """
    百度云期货tick csv数据入库, 只落库rb
    :param path: 百度云下载文件路径
    :param year: 年份
    :param symbol_type: 品种
    :return:
    """
    trading_day_dirs = os.listdir(os.path.join(path, year))
    symbol_type_csvs = []
    for trading_day_dir in trading_day_dirs:
        symbol_csvs = os.listdir(os.path.join(path, year, trading_day_dir))
        for symbol_csv in symbol_csvs:
            if symbol_csv.startswith(symbol_type):
                symbol_type_csvs.append(os.path.join(path, year, trading_day_dir, symbol_csv))
    for csv in tqdm.tqdm(symbol_type_csvs):
        csv_df = pl.read_csv(csv)
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
                pl.col("InstrumentID").str.replace_all(r"\d+", "").alias("symbol_type"),
                pl.col("InstrumentID").alias("exchange_symbol"),
                pl.lit("SHFE").alias("exchange")
            )
            .rename({
                "TradingDay": "trading_day",
                "InstrumentID": "symbol",
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
        )

        db.engine.FUTURE_DB_ORIGIN.insert_df(db.table.FUTURE_TICK, csv_df_clean.to_pandas())


def future_tick_to_future_rb_tick():
    """
    future_tick表中rb数据写入rb专门的表
    :return:
    """
    pl.read_database(
        f"""
        SELECT *
        FROM {db.table.FUTURE_TICK}
        """,
        db.engine.FUTURE_DB,
        iter_batches=True,
    )