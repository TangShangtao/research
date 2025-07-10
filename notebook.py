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

for rb_main_tick in market_data.read_rb_main_tick(start_trading_day, end_trading_day, "1mo"):
    factor = research.factors.tick_to_min1.orderbook_imbalance(rb_main_tick)
    break

main_min1 = market_data.read_future_main_min1(start_trading_day, end_trading_day)

test = (
    factor
    .with_columns(
        factor_z_score = (
            (pl.col("factor") - pl.col("factor").rolling_mean(window_size=5)) /
            pl.col("factor").rolling_std(window_size=5)
        ).fill_null(0).tanh()
    )
)