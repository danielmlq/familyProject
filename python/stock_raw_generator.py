import os
import sys
import shutil
from configobj import ConfigObj
import pandas_datareader as web
import numpy as np

class stock_raw_generator:
    def load_stock_data(stock, start_date, end_date):
        return web.DataReader(stock, data_source='yahoo', start=start_date, end=end_date)

    def add_symbol_column(input_df, symbol):
        input_df['symbol'] = symbol

    def add_date_column(input_df):
        input_df['date'] = input_df.index

    def add_change_percentage_column(input_df):
        input_df['changePercentage'] = np.round(((input_df['Close'] / input_df['Close'].shift(1)) - 1) * 100, 2)

    # Clear all the rows contain NAN values
    def clear_nan_rows(input_df, col_name):
        return input_df.loc[(input_df[col_name].notnull())]

    def add_trend_column(input_df):
        input_df['go_up'] = input_df['changePercentage'] > 0.0

    def write_header(raw_output_dir):
        f = open("{}/output.csv".format(raw_output_dir), "a")
        f.write("High,Low,Open,Close,Volume,Adj Close,symbol,date,changePercentage,go_up\n")
        f.close()

    if __name__ == '__main__':
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
            add_change_percentage_column(raw_df)
            add_trend_column(raw_df)
            raw_df = clear_nan_rows(raw_df, 'changePercentage')
            raw_df.to_csv("{}/output.csv".format(raw_output_dir), encoding='utf-8', mode='a', header=False, index=False)
            # print(raw_df)