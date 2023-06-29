import sys
import psycopg2
import psycopg2.extras
from configobj import ConfigObj
from datetime import datetime

cfg_file = sys.argv[1]
cfg = ConfigObj(cfg_file)

db_name = cfg.get("DB_NAME")
db_host = cfg.get("DB_HOST")
db_username = cfg.get("DB_USER")
db_password = cfg.get("DB_PWD")
stock_symbols = cfg.get("STOCK_SYMBOLS")
president = cfg.get("PRESIDENT")

class stock_statistics_entity:
    def __init__(self, statistics_id, stock_symbol, statistics_date, president, max_days_go_up, max_percentage_go_up, avg_days_go_up, avg_percentage_go_up, max_days_go_down, max_percentage_go_down, avg_days_go_down, avg_percentage_go_down):
        self.statistics_id = statistics_id
        self.stock_symbol = stock_symbol
        self.statistics_date = statistics_date
        self.president = president
        self.max_days_go_up = max_days_go_up
        self.max_percentage_go_up = max_percentage_go_up
        self.avg_days_go_up = avg_days_go_up
        self.avg_percentage_go_up = avg_percentage_go_up
        self.max_days_go_down = max_days_go_down
        self.max_percentage_go_down = max_percentage_go_down
        self.avg_days_go_down = avg_days_go_down
        self.avg_percentage_go_down = avg_percentage_go_down

class stock_statistics_generator:
    def __init__(self):
        self.conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(db_host, db_name, db_username, db_password)
        self.conn = psycopg2.connect(self.conn_string)
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def if_stock_analysis_is_available(self, stock_symbol):
        query_string = "SELECT * FROM stock_analysis WHERE stock_symbol = '{}' AND end_date > '2021-01-19'".format(stock_symbol)
        self.cursor.execute(query_string)
        return self.cursor.fetchall()

    def generate_stock_statistics(self, stock_symbol, president):
        curr_date = datetime.today().strftime('%Y-%m-%d')

        query_string = "SELECT * FROM stock_statistics WHERE stock_symbol = '{}' AND president = '{}'".format(stock_symbol, president)
        self.cursor.execute(query_string)
        latest_stock_analysis = self.cursor.fetchone()
        if latest_stock_analysis is None:
            entity = stock_statistics_entity(None, stock_symbol, curr_date, president, None, None, None, None, None, None, None, None)
        else:
            entity = stock_statistics_entity(latest_stock_analysis[0], latest_stock_analysis[1], latest_stock_analysis[2], latest_stock_analysis[3], latest_stock_analysis[4], latest_stock_analysis[5], latest_stock_analysis[6], latest_stock_analysis[7], latest_stock_analysis[8], latest_stock_analysis[9], latest_stock_analysis[10], latest_stock_analysis[11])

        query_string = "SELECT go_up, avg(total_number_of_days) as avg_number_of_days, avg(total_percentage) as avg_percentage FROM stock_analysis WHERE stock_symbol = '{}' and end_date > '2021-01-19' and total_number_of_days > 1 group by go_up".format(stock_symbol)
        self.cursor.execute(query_string)
        avg_results = self.cursor.fetchall()
        for avg_result in avg_results:
            if avg_result[0] is True:
                entity.avg_days_go_up = avg_result[1]
                entity.avg_percentage_go_up = avg_result[2]
            else:
                entity.avg_days_go_down = avg_result[1]
                entity.avg_percentage_go_down = avg_result[2]

        query_string = "SELECT max(total_number_of_days) as max_days_go_up, max(total_percentage) as max_percentage_go_up from stock_analysis where stock_symbol = '{}' and end_date > '2021-01-19' and go_up is true".format(stock_symbol)
        self.cursor.execute(query_string)
        max_result = self.cursor.fetchone()
        entity.max_days_go_up = max_result[0]
        entity.max_percentage_go_up = max_result[1]

        query_string = "SELECT max(total_number_of_days) as max_days_go_down, max(total_percentage) as max_percentage_go_down from stock_analysis where stock_symbol = '{}' and end_date > '2021-01-19' and go_up is false".format(stock_symbol)
        self.cursor.execute(query_string)
        min_result = self.cursor.fetchone()
        entity.max_days_go_down = min_result[0]
        entity.max_percentage_go_down = min_result[1]

        if entity.statistics_id is None:
            query_string = "INSERT INTO stock_statistics(stock_symbol, statistics_date, president, max_days_go_up, max_percentage_go_up, avg_days_go_up, avg_percentage_go_up, max_days_go_down, max_percentage_go_down, avg_days_go_down, avg_percentage_go_down) VALUES ('{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {})" \
                .format(entity.stock_symbol, entity.statistics_date, entity.president, entity.max_days_go_up, entity.max_percentage_go_up, entity.avg_days_go_up, entity.avg_percentage_go_up, entity.max_days_go_down, entity.max_percentage_go_down, entity.avg_days_go_down, entity.avg_percentage_go_down)
        else:
            query_string = "UPDATE stock_statistics SET stock_symbol = '{}', statistics_date = '{}', president = '{}', max_days_go_up = {}, max_percentage_go_up = {}, avg_days_go_up = {}, avg_percentage_go_up = {}, max_days_go_down = {}, max_percentage_go_down = {}, avg_days_go_down = {}, avg_percentage_go_down = {} WHERE statistics_id = {}" \
                .format(entity.stock_symbol, entity.statistics_date, entity.president, entity.max_days_go_up, entity.max_percentage_go_up, entity.avg_days_go_up, entity.avg_percentage_go_up, entity.max_days_go_down, entity.max_percentage_go_down, entity.avg_days_go_down, entity.avg_percentage_go_down, entity.statistics_id)
        print("QUERY: {}".format(query_string))
        self.cursor.execute(query_string)
        self.conn.commit()

if __name__ == "__main__":
    # Initialization
    generator = stock_statistics_generator()
    generator.__init__()
    stock_symbols_arr = stock_symbols.split(',')

    for stock_symbol in stock_symbols_arr:
        if len(generator.if_stock_analysis_is_available(stock_symbol)) == 0:
            print("No records available in stock_analysis for stock_symbol: {}".format(stock_symbol))
            continue
        generator.generate_stock_statistics(stock_symbol, president)