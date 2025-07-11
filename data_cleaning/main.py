from data_cleaning import fetch_data, agg_data
import loguru
if __name__ == "__main__":
    for year in range(2011,2020):
        loguru.logger.info(f"start year {year}")
        fetch_data.zip_baiduyun_tick_file("E:\\BaiduNetdiskDownload", str(year))
        fetch_data.future_tick_csv_to_ck("E:\\BaiduNetdiskDownload", str(year))
