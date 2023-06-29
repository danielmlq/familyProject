import sys
import psycopg2
import psycopg2.extras
from configobj import ConfigObj

cfg_file = sys.argv[1]
cfg = ConfigObj(cfg_file)

db_name = cfg.get("DB_NAME")
db_host = cfg.get("DB_HOST")
db_username = cfg.get("DB_USER")
db_password = cfg.get("DB_PWD")
stock_symbols = cfg.get("STOCK_SYMBOLS")

class stock_analysis_entity:
    def __init__(self, analysis_id, stock_symbol, start_date, end_date, go_up, total_number_of_days, total_percentage):
        self.analysis_id = analysis_id
        self.stock_symbol = stock_symbol
        self.start_date = start_date
        self.end_date = end_date
        self.go_up = go_up
        self.total_number_of_days = total_number_of_days
        self.total_percentage = total_percentage

class stock_raw_entity:
    def __init__(self, raw_id, stock_symbol, raw_date, open, high, low, close, adjust_close, volume, change_percentage, go_up):
        self.raw_id = raw_id
        self.stock_symbol = stock_symbol
        self.raw_date = raw_date
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.adjust_close = adjust_close
        self.volume = volume
        self.change_percentage = change_percentage
        self.go_up = go_up

class stock_analysis_generator:
    def __init__(self):
        self.conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(db_host, db_name, db_username, db_password)
        self.conn = psycopg2.connect(self.conn_string)
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def get_existing_stock_analysis(self, stock_symbol):
        # query_string = "SELECT * FROM stock_analysis WHERE stock_symbol = '{}' and start_date = (SELECT MAX(start_date) FROM stock_analysis WHERE stock_symbol = '{}')".format(stock_symbol, stock_symbol)
        query_string = "SELECT * FROM stock_analysis WHERE stock_symbol = '{}' and start_date > (SELECT MAX(start_date) FROM stock_analysis WHERE stock_symbol = '{}')".format(stock_symbol, stock_symbol)
        self.cursor.execute(query_string)
        return self.cursor.fetchone()

    def load_stock_raw(self, latest_stock_analysis_row, stock_symbol):
        if latest_stock_analysis_row is None:
            # No stock_analyis records available, load all stock_raw records for this stock.
            query = "SELECT * FROM stock_raw WHERE stock_symbol = '{}' AND raw_date > '2021-01-19'".format(stock_symbol)
        else:
            current_stock_analysis_row = stock_analysis_entity(latest_stock_analysis_row[0], latest_stock_analysis_row[1], latest_stock_analysis_row[2], latest_stock_analysis_row[3], latest_stock_analysis_row[4], latest_stock_analysis_row[5], latest_stock_analysis_row[6])
            query = "SELECT * FROM stock_raw WHERE stock_symbol = '{}' AND raw_date > '{}'".format(stock_symbol, current_stock_analysis_row.end_date)
        # print("QUERY: {}".format(query))
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def persist_latest_stock_analysis(self, latest_stock_analysis_row):
        # New stock_analysis row, use insert DML
        if latest_stock_analysis_row.analysis_id is None:
            query = "INSERT INTO stock_analysis(stock_symbol, start_date, end_date, go_up, total_number_of_days, total_percentage) VALUES ('{}', '{}', '{}', {}, {}, {})" \
                .format(latest_stock_analysis_row.stock_symbol, latest_stock_analysis_row.start_date, latest_stock_analysis_row.end_date, latest_stock_analysis_row.go_up, latest_stock_analysis_row.total_number_of_days, latest_stock_analysis_row.total_percentage)
        # Existing stock_analysis row, use update DML
        else:
            query = "UPDATE stock_analysis SET end_date = '{}', total_number_of_days = {}, total_percentage = {} WHERE analysis_id = {}"\
                .format(latest_stock_analysis_row.end_date, latest_stock_analysis_row.total_number_of_days, latest_stock_analysis_row.total_percentage, latest_stock_analysis_row.analysis_id)
        # print("QUERY: {}".format(query))
        self.cursor.execute(query)
        self.conn.commit()

if __name__ == "__main__":
    # Initialization
    generator = stock_analysis_generator()
    generator.__init__()
    stock_symbols_arr = stock_symbols.split(',')
    for stock_symbol in stock_symbols_arr:
        # Get latest result form stock_analysis
        latest_stock_analysis_row = generator.get_existing_stock_analysis(stock_symbol)
        # Construct latest stock_analysis_entity
        if latest_stock_analysis_row is None:
            input_stock_analysis_row = stock_analysis_entity(None, None, None, None, None, 0, 0)
        else:
            input_stock_analysis_row = stock_analysis_entity(latest_stock_analysis_row[0], latest_stock_analysis_row[1], latest_stock_analysis_row[2], latest_stock_analysis_row[3], latest_stock_analysis_row[4], latest_stock_analysis_row[5], latest_stock_analysis_row[6])

        # Load stock_raw rows based on the latest stock_analysis row.
        stock_raw_rows = generator.load_stock_raw(latest_stock_analysis_row, stock_symbol)
        for stock_raw_row in stock_raw_rows:
            stock_raw_input_row = stock_raw_entity(stock_raw_row[0], stock_raw_row[1], stock_raw_row[2], stock_raw_row[3], stock_raw_row[4], stock_raw_row[5], stock_raw_row[6], stock_raw_row[7], stock_raw_row[8], stock_raw_row[9], stock_raw_row[10])
            # Nothing load, just assign the corresponding values from first stock_raw_rwo to latest_stock_analysis_row
            if input_stock_analysis_row.total_number_of_days == 0:
                input_stock_analysis_row.stock_symbol = stock_raw_input_row.stock_symbol
                input_stock_analysis_row.start_date = stock_raw_input_row.raw_date
                input_stock_analysis_row.end_date = stock_raw_input_row.raw_date
                input_stock_analysis_row.go_up = stock_raw_input_row.go_up
                input_stock_analysis_row.total_number_of_days += 1
                input_stock_analysis_row.total_percentage += abs(stock_raw_input_row.change_percentage)
            # Next stock_raw record go_up value is same as latest stock analysis -> just accumulate the result
            elif input_stock_analysis_row.go_up == stock_raw_input_row.go_up:
                input_stock_analysis_row.end_date = stock_raw_input_row.raw_date
                input_stock_analysis_row.total_number_of_days += 1
                input_stock_analysis_row.total_percentage += abs(stock_raw_input_row.change_percentage)
            # Next stock_raw record go_up value differs from latest stock analysis -> persist the existing to db (insert or update), reset stock analysis for next round.
            else:
                generator.persist_latest_stock_analysis(input_stock_analysis_row)
                input_stock_analysis_row = stock_analysis_entity(None, stock_raw_input_row.stock_symbol, stock_raw_input_row.raw_date, stock_raw_input_row.raw_date, stock_raw_input_row.go_up, 1, abs(stock_raw_input_row.change_percentage))

        # Persist the last stock analysis to database which may variate in the next run.
        if input_stock_analysis_row.total_number_of_days != 0:
            generator.persist_latest_stock_analysis(input_stock_analysis_row)