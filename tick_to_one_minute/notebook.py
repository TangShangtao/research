import db
import polars as pl
import pandas as pd
import datetime as dt
import time


start = time.time()
test = pl.read_database(
    f"""
    SELECT *
    FROM {db.table.FUTURE_TICK}
    WHERE trading_day = '{dt.date(2021,1,5)}'
    AND symbol = 'rb2102'
    ORDER BY tick_time
    """,
    db.engine.FUTURE_DB,
)
end = time.time()
print(end - start)