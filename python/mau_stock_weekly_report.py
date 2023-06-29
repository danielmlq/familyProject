import sys
import psycopg2
import psycopg2.extras
from configobj import ConfigObj
import numpy as np

cfg_file = sys.argv[1]
cfg = ConfigObj(cfg_file)

db_name = cfg.get("DB_NAME")
db_host = cfg.get("DB_HOST")
db_username = cfg.get("DB_USER")
db_password = cfg.get("DB_PWD")
stock_symbols = cfg.get("STOCK_SYMBOLS")

class mau_stock_raw_entity:
    def __init__(self, raw_id, raw_date, day_index_of_week, day_of_week, symbol, open, high, low, close, adjust_close, volume, difference_value, change_percentage, go_up, price_range):
        self.raw_id = raw_id
        self.raw_date = raw_date
        self.day_index_of_week = day_index_of_week
        self.day_of_week = day_of_week
        self.symbol = symbol
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.adjust_close = adjust_close
        self.volume = volume
        self.difference_value = difference_value
        self.change_percentage = change_percentage
        self.go_up = go_up
        self.price_range = price_range

class mau_stock_weekly_report_entity:
    def __init__(self, weekly_report_id, stock_symbol, start_date, end_date, total_number_of_days, avg_open, avg_close, difference_value, total_percentage, go_up, price_range, end_day_index_of_week):
        self.weekly_report_id = weekly_report_id
        self.stock_symbol = stock_symbol
        self.start_date = start_date
        self.end_date = end_date
        self.total_number_of_days = total_number_of_days
        self.avg_open = avg_open
        self.avg_close = avg_close
        self.difference_value = difference_value
        self.total_percentage = total_percentage
        self.go_up = go_up
        self.price_range = price_range
        self.end_day_index_of_week = end_day_index_of_week

class mau_stock_weekly_report_generator:
    def __init__(self):
        self.conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(db_host, db_name, db_username, db_password)
        self.conn = psycopg2.connect(self.conn_string)
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def get_existing_stock_weekly_report_record(self, stock_symbol):
        query_string = "SELECT * FROM mau_stock_weekly_report WHERE stock_symbol = '{}' and start_date = (SELECT MAX(start_date) FROM mau_stock_weekly_report WHERE stock_symbol = '{}')".format(stock_symbol, stock_symbol)
        print("QUERY: {}".format(query_string))
        self.cursor.execute(query_string)
        return self.cursor.fetchone()

    def load_stock_raw(self, input_stock_weekly_report_entity, stock_symbol):
        if input_stock_weekly_report_entity.weekly_report_id is None:
            # No weekly_report records available, load all stock_raw records for this stock.
            query = "SELECT * FROM mau_stock_raw WHERE symbol = '{}' order by raw_date asc".format(stock_symbol)
        else:
            query = "SELECT * FROM mau_stock_raw WHERE symbol = '{}' AND raw_date > '{}' order by raw_date asc".format(stock_symbol, input_stock_weekly_report_entity.end_date)
        print("QUERY: {}".format(query))
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def persist_mau_stock_weely_reoprt_record_to_db(self, input_stock_weekly_report_entity):
        if input_stock_weekly_report_entity.weekly_report_id is None:
            query_string = "INSERT INTO mau_stock_weekly_report(stock_symbol, start_date, end_date, total_number_of_days, avg_open, avg_close, difference_value, total_percentage, go_up, price_range, end_day_index_of_week) VALUES ('{}', '{}', '{}', {}, {}, {}, {}, {}, {}, '{}', {})"\
                .format(input_stock_weekly_report_entity.stock_symbol, input_stock_weekly_report_entity.start_date, input_stock_weekly_report_entity.end_date, input_stock_weekly_report_entity.total_number_of_days, input_stock_weekly_report_entity.avg_open, input_stock_weekly_report_entity.avg_close, input_stock_weekly_report_entity.difference_value, input_stock_weekly_report_entity.total_percentage, input_stock_weekly_report_entity.go_up, input_stock_weekly_report_entity.price_range, input_stock_weekly_report_entity.end_day_index_of_week)
        else:
            query_string = "UPDATE mau_stock_weekly_report SET end_date = '{}', total_number_of_days = {}, avg_open = {}, avg_close = {}, difference_value = {}, total_percentage = {}, go_up = {}, price_range = '{}', end_day_index_of_week = {} WHERE weekly_report_id = {}"\
                .format(input_stock_weekly_report_entity.end_date, input_stock_weekly_report_entity.total_number_of_days, input_stock_weekly_report_entity.avg_open, input_stock_weekly_report_entity.avg_close, input_stock_weekly_report_entity.difference_value, input_stock_weekly_report_entity.total_percentage, input_stock_weekly_report_entity.go_up, input_stock_weekly_report_entity.price_range, input_stock_weekly_report_entity.end_day_index_of_week, input_stock_weekly_report_entity.weekly_report_id)
        # print("QUERY: {}".format(query_string))
        self.cursor.execute(query_string)
        self.conn.commit()

    # Update current mau_stock_weekly_report entity, if full week, then persist to db and initialize new one.
    def update_mau_stock_weekly_report_entity(self, input_stock_weekly_report_entity, stock_raw_rows):
        for stock_raw_row in stock_raw_rows:
            raw_entity = mau_stock_raw_entity(stock_raw_row[0], stock_raw_row[1], stock_raw_row[2], stock_raw_row[3], stock_raw_row[4], stock_raw_row[5], stock_raw_row[6], stock_raw_row[7], stock_raw_row[8], stock_raw_row[9], stock_raw_row[10], stock_raw_row[11], stock_raw_row[12], stock_raw_row[13], stock_raw_row[14])
            if input_stock_weekly_report_entity.end_day_index_of_week <= raw_entity.day_index_of_week:
                if input_stock_weekly_report_entity.stock_symbol is None:
                    input_stock_weekly_report_entity.stock_symbol = raw_entity.symbol
                if input_stock_weekly_report_entity.start_date is None:
                    input_stock_weekly_report_entity.start_date = raw_entity.raw_date
                input_stock_weekly_report_entity.end_date = raw_entity.raw_date
                input_stock_weekly_report_entity.avg_open = np.round((input_stock_weekly_report_entity.avg_open * input_stock_weekly_report_entity.total_number_of_days + raw_entity.open) / (input_stock_weekly_report_entity.total_number_of_days + 1), 2)
                input_stock_weekly_report_entity.avg_close = np.round((input_stock_weekly_report_entity.avg_close * input_stock_weekly_report_entity.total_number_of_days + raw_entity.close) / (input_stock_weekly_report_entity.total_number_of_days + 1), 2)
                input_stock_weekly_report_entity.total_number_of_days += 1
                input_stock_weekly_report_entity.difference_value = np.round(input_stock_weekly_report_entity.avg_close - input_stock_weekly_report_entity.avg_open, 2)
                input_stock_weekly_report_entity.total_percentage = np.round(input_stock_weekly_report_entity.total_percentage + raw_entity.change_percentage, 2)
                input_stock_weekly_report_entity.go_up = True if input_stock_weekly_report_entity.total_percentage > 0 else False
                input_stock_weekly_report_entity.end_day_index_of_week = raw_entity.day_index_of_week

                if input_stock_weekly_report_entity.price_range is None:
                    min_price = sys.maxsize
                    max_price = -sys.maxsize - 1
                else:
                    min_max_prices = input_stock_weekly_report_entity.price_range.split('~')
                    min_price = np.round(float(min_max_prices[0]), 2)
                    max_price = np.round(float(min_max_prices[1]), 2)
                low_price = np.round(raw_entity.low, 2)
                high_price = np.round(raw_entity.high, 2)
                min_price = low_price if low_price < min_price else min_price
                max_price = high_price if high_price > max_price else max_price
                input_stock_weekly_report_entity.price_range = '{}~{}'.format(min_price, max_price)
            else:
                generator.persist_mau_stock_weely_reoprt_record_to_db(input_stock_weekly_report_entity)
                input_stock_weekly_report_entity.weekly_report_id = None
                input_stock_weekly_report_entity.stock_symbol = raw_entity.symbol
                input_stock_weekly_report_entity.start_date = raw_entity.raw_date
                input_stock_weekly_report_entity.end_date = raw_entity.raw_date
                input_stock_weekly_report_entity.total_number_of_days = 1
                input_stock_weekly_report_entity.avg_open = np.round(raw_entity.open, 2)
                input_stock_weekly_report_entity.avg_close = np.round(raw_entity.close, 2)
                input_stock_weekly_report_entity.difference_value = np.round(input_stock_weekly_report_entity.avg_close - input_stock_weekly_report_entity.avg_open, 2)
                input_stock_weekly_report_entity.total_percentage = np.round(raw_entity.change_percentage, 2)
                input_stock_weekly_report_entity.go_up = True if input_stock_weekly_report_entity.total_percentage > 0 else False
                input_stock_weekly_report_entity.price_range = '{}~{}'.format(np.round(raw_entity.low, 2), np.round(raw_entity.high, 2))
                input_stock_weekly_report_entity.end_day_index_of_week = raw_entity.day_index_of_week

        if input_stock_weekly_report_entity.stock_symbol is not None:
            generator.persist_mau_stock_weely_reoprt_record_to_db(input_stock_weekly_report_entity)

if __name__ == "__main__":
    # Initialization
    generator = mau_stock_weekly_report_generator()
    generator.__init__()
    stock_symbols_arr = stock_symbols.split(',')
    for stock_symbol in stock_symbols_arr:
        # Get latest result form mau_stock_weekly_report
        latest_stock_weekly_report_row = generator.get_existing_stock_weekly_report_record(stock_symbol)
        # Construct latest stock_weekly_report_entity
        if latest_stock_weekly_report_row is None:
            input_stock_weekly_report_entity = mau_stock_weekly_report_entity(None, None, None, None, 0, 0, 0, 0, 0, None, None, 0)
        else:
            input_stock_weekly_report_entity = mau_stock_weekly_report_entity(latest_stock_weekly_report_row[0], latest_stock_weekly_report_row[1], latest_stock_weekly_report_row[2], latest_stock_weekly_report_row[3], latest_stock_weekly_report_row[4], latest_stock_weekly_report_row[5], latest_stock_weekly_report_row[6], latest_stock_weekly_report_row[7], latest_stock_weekly_report_row[8], latest_stock_weekly_report_row[9], latest_stock_weekly_report_row[10], latest_stock_weekly_report_row[11])
        # Load stock_raw rows based on the latest stock_analysis row.
        stock_raw_rows = generator.load_stock_raw(input_stock_weekly_report_entity, stock_symbol)
        # Update current mau_stock_weekly_report entity, if full week, then persist to db and initialize new one.
        generator.update_mau_stock_weekly_report_entity(input_stock_weekly_report_entity, stock_raw_rows)