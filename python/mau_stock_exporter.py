import os
import sys
import shutil
import psycopg2
import psycopg2.extras
from configobj import ConfigObj
import pandas as pd

cfg_file = sys.argv[1]
current_dt = sys.argv[2]
cfg = ConfigObj(cfg_file)

db_name = cfg.get("DB_NAME")
db_host = cfg.get("DB_HOST")
db_username = cfg.get("DB_USER")
db_password = cfg.get("DB_PWD")
stock_symbols = cfg.get("STOCK_SYMBOLS")
output_dir = cfg.get("OUTPUT_DIR")

class stock_info_exporter:
    def __init__(self):
        self.conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(db_host, db_name, db_username, db_password)
        self.conn = psycopg2.connect(self.conn_string)
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def get_top20_stickers(self, input_date):
        query_string = "select symbol from mau_stock_raw where raw_date = '{}' order by volume desc limit 10".format(input_date)
        self.cursor.execute(query_string)
        return self.cursor.fetchall()

    def get_stock_raw_dataFrame(self, sticker):
        df_columns = ['raw_date', 'day_of_week', 'symbol', 'open', 'high', 'low', 'close', 'adjust_close', 'volume', 'difference_value', 'change_percentage', 'go_up', 'price_range']
        query_string = "select raw_date, day_of_week, symbol, open, high, low, close, adjust_close, volume, difference_value, change_percentage, go_up, price_range from mau_stock_raw where symbol = '{}' order by raw_date desc limit 100".format(sticker[0])
        # print(query_string)
        self.cursor.execute(query_string)
        rows = self.cursor.fetchall()
        return pd.DataFrame(rows, columns=df_columns)

    def get_stock_analysis_dataFrame(self, sticker):
        df_columns = ['symbol', 'start_date', 'end_date', 'go_up', 'total_number_of_days', 'total_percentage', 'avg_daily_percentage', 'price_range']
        query_string = "select stock_symbol, start_date, end_date, go_up, total_number_of_days, total_percentage, avg_daily_percentage, price_range from mau_stock_analysis where stock_symbol = '{}' order by end_date desc limit 100".format(sticker[0])
        # print(query_string)
        self.cursor.execute(query_string)
        rows = self.cursor.fetchall()
        return pd.DataFrame(rows, columns=df_columns)

    def get_stock_weekly_dataFrame(self, sticker):
        df_columns = ['symbol', 'start_date', 'end_date', 'total_number_of_days', 'avg_open', 'avg_close', 'difference_value', 'total_percentage', 'go_up', 'price_range']
        query_string = "select stock_symbol, start_date, end_date, total_number_of_days, avg_open, avg_close, difference_value, total_percentage, go_up, price_range from mau_stock_weekly_report where stock_symbol = '{}' order by end_date desc limit 100".format(sticker[0])
        # print(query_string)
        self.cursor.execute(query_string)
        rows = self.cursor.fetchall()
        return pd.DataFrame(rows, columns=df_columns)

    def get_stock_monthly_dataFrame(self, sticker):
        df_columns = ['symbol', 'start_date', 'end_date', 'total_number_of_days', 'avg_open', 'avg_close', 'difference_value', 'total_percentage', 'go_up', 'price_range']
        query_string = "select stock_symbol, start_date, end_date, total_number_of_days, avg_open, avg_close, difference_value, total_percentage, go_up, price_range from mau_stock_monthly_report where stock_symbol = '{}' order by end_date desc limit 100".format(sticker[0])
        # print(query_string)
        self.cursor.execute(query_string)
        rows = self.cursor.fetchall()
        return pd.DataFrame(rows, columns=df_columns)

    def get_stock_quarterly_dataFrame(self, sticker):
        df_columns = ['symbol', 'start_date', 'end_date', 'total_number_of_days', 'avg_open', 'avg_close', 'difference_value', 'total_percentage', 'go_up', 'price_range']
        query_string = "select stock_symbol, start_date, end_date, total_number_of_days, avg_open, avg_close, difference_value, total_percentage, go_up, price_range from mau_stock_quarterly_report where stock_symbol = '{}' order by end_date desc limit 100".format(sticker[0])
        # print(query_string)
        self.cursor.execute(query_string)
        rows = self.cursor.fetchall()
        return pd.DataFrame(rows, columns=df_columns)

if __name__ == "__main__":
    # Initialization
    exporter = stock_info_exporter()
    exporter.__init__()
    output_directory = "{}/{}".format(output_dir, current_dt)
    stickers = exporter.get_top20_stickers(current_dt)
    # stickers = ["^GSPC"]
    # Create output directory
    if os.path.exists(output_directory):
        shutil.rmtree(output_directory)
    os.mkdir(output_directory)

    for sticker in stickers:
        # print("stocker:{}".format(sticker))
        sub_dir = "{}/{}".format(output_directory, sticker[0])
        # sub_dir = "{}/{}".format(output_directory, sticker)
        os.mkdir(sub_dir)
        df = exporter.get_stock_raw_dataFrame(sticker)
        df.to_csv("{}/raw.csv".format(sub_dir), index=False)

        df = exporter.get_stock_analysis_dataFrame(sticker)
        df.to_csv("{}/analysis.csv".format(sub_dir), index=False)

        df = exporter.get_stock_weekly_dataFrame(sticker)
        df.to_csv("{}/weekly.csv".format(sub_dir), index=False)

        df = exporter.get_stock_monthly_dataFrame(sticker)
        df.to_csv("{}/monthly.csv".format(sub_dir), index=False)

        df = exporter.get_stock_quarterly_dataFrame(sticker)
        df.to_csv("{}/quarterly.csv".format(sub_dir), index=False)