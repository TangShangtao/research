from data_cleaning import fetch_data, agg_data
import loguru
if __name__ == "__main__":

    # fetch_data.zip_baiduyun_tick_file("E:\\BaiduNetdiskDownload", "2019")
    # fetch_data.future_tick_csv_to_ck("E:\\BaiduNetdiskDownload", 2020, 2020)
    fetch_data.future_tick_to_future_main_tick(2021)