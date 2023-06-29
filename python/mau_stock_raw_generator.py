import os
import sys
import shutil
import psycopg2
import psycopg2.extras
from configobj import ConfigObj
import pandas_datareader as web
import numpy as np
import calendar

cfg_file = sys.argv[1]
cfg = ConfigObj(cfg_file)

db_name = cfg.get("DB_NAME")
db_host = cfg.get("DB_HOST")
db_username = cfg.get("DB_USER")
db_password = cfg.get("DB_PWD")
raw_output_dir = cfg.get("RAW_OUTPUT_DIR")
stock_symbols = cfg.get("STOCK_SYMBOLS")
start_date = sys.argv[2]
end_date = sys.argv[3]

class stock_raw_generator:
    def __init__(self):
        self.conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(db_host, db_name, db_username, db_password)
        self.conn = psycopg2.connect(self.conn_string)
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def load_stock_data(stock, start_date, end_date):
        return web.DataReader(stock, data_source='yahoo', start=start_date, end=end_date)

    def add_symbol_column(input_df, symbol):
        input_df['symbol'] = symbol

    def add_date_column(input_df):
        input_df['date'] = input_df.index

    def add_day_of_week_column(input_df):
        input_df['day_index_of_week'] = input_df['date'].apply(lambda row: row.weekday())

    def add_weekday_column(input_df):
        input_df['day_of_week'] = input_df['day_index_of_week'].apply(lambda row: calendar.day_name[row])

    def add_change_column(input_df):
        input_df['change'] = input_df['Open'] - input_df['Close'].shift(1)

    def add_change_percentage_column(input_df):
        input_df['changePercentage'] = np.round(((input_df['Open'] / input_df['Close'].shift(1)) - 1) * 100, 2)

    # Clear all the rows contain NAN values
    def clear_nan_rows(input_df, col_name):
        return input_df.loc[(input_df[col_name].notnull())]

    def add_trend_column(input_df):
        input_df['go_up'] = input_df['changePercentage'] > 0.0

    def add_range_column(input_df):
        cols = ['Low', 'High']
        input_df['Range'] = input_df[cols].apply(lambda row: '~'.join(np.round(row.values, 2).astype(str)), axis=1)

    def write_header(raw_output_dir):
        f = open("{}/output.csv".format(raw_output_dir), "a")
        f.write("high,low,open,close,volume,adj_close,symbol,date,day_index_of_week,day_of_week,change,change_percentage,go_up,range\n")
        f.close()

    # def get_curr_latest_raw_date(self):
    #     query_string = "SELECT MAX(RAW_DATE) FROM mau_stock_raw"
    #     self.cursor.execute(query_string)
    #     return self.cursor.fetchone()

    if __name__ == '__main__':
        # start_date = get_curr_latest_raw_date()
        # print("start_date {}".format(start_date))

        # Remove pre-exist data and recreate directory
        if os.path.exists(raw_output_dir):
            shutil.rmtree(raw_output_dir)
        os.mkdir(raw_output_dir)
        # Create file and insert header
        write_header(raw_output_dir)

        # Get stock symbol array
        stock_symbols_arr = stock_symbols.split(',')
        for stock_symbol in stock_symbols_arr:
            try:
                raw_df = load_stock_data(stock_symbol, start_date, end_date)
            except:
                print("Throws exception while processing stock: {}, skip to next".format(stock_symbol))
                continue
            add_symbol_column(raw_df, stock_symbol)
            add_date_column(raw_df)
            add_day_of_week_column(raw_df)
            add_weekday_column(raw_df)
            add_change_column(raw_df)
            add_change_percentage_column(raw_df)
            add_trend_column(raw_df)
            add_range_column(raw_df)
            raw_df = clear_nan_rows(raw_df, 'changePercentage')
            raw_df.to_csv("{}/output.csv".format(raw_output_dir), encoding='utf-8', mode='a', header=False, index=False)
            # print(raw_df)

