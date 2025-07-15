import db
import polars as pl
import pandas as pd
import numpy as np
import datetime as dt
from public import market_data, future_basic_info
import research
from tqdm import tqdm


start_trading_day = dt.date(2021,1,4)
end_trading_day = dt.date(2021,12,31)
initial_capital = 10000
strategy_name = "test"
symbol_type = "AP"

# pre_balance + net_deposit + close_profit + hold_profit - commission = balance
# balance - margin = available
account = pl.DataFrame([{
    "trading_day":start_trading_day,
    "bar_time":dt.datetime.combine(start_trading_day, dt.time(0,0,0)),
    "strategy_name":strategy_name,
    "pre_balance":0,
    "net_deposit":initial_capital,
    "commission":0,
    "close_profit":0,
    "hold_profit":0,
    "balance":initial_capital,
    "margin":0,
    "available":initial_capital
}])
position = pl.DataFrame([{
    "trading_day":start_trading_day,
    "bar_time":dt.datetime.combine(start_trading_day, dt.time(0,0,0)),
    "strategy_name":strategy_name,

}])
data_iter, data_len = market_data.read_tick(start_trading_day, end_trading_day, symbol_type, "1mo")
for tick in tqdm(data_iter, total=data_len):
    factor = research.factors.tick_to_min1.orderbook_imbalance(tick)
    break

main_min1 = market_data.read_main_min1(start_trading_day, end_trading_day, symbol_type)