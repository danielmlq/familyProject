import os
import sys
from configobj import ConfigObj
import psycopg2
import psycopg2.extras
import csv

cfg_file = sys.argv[1]
cfg = ConfigObj(cfg_file)

db_name = cfg.get("DB_NAME")
db_host = cfg.get("DB_HOST")
db_username = cfg.get("DB_USER")
db_password = cfg.get("DB_PWD")
input_path = cfg.get("RAW_INPUT_DIR")


class online_sale_item:
    def __init__(self, asin, product_name, brand, category, est_monthly_revenue, est_monthly_sales, price, fees, net,
                 rank, reviews, lqs, sellers, date_first_available, buy_box_owner, rating, dimensions, product_tier,
                 weight_in_lbs):
        self.asin = asin
        self.product_name = product_name
        self.brand = brand
        self.category = category
        self.est_monthly_revenue = est_monthly_revenue
        self.est_monthly_sales = est_monthly_sales
        self.price = price
        self.fees = fees
        self.net = net
        self.rank = rank
        self.reviews = reviews
        self.lqs = lqs
        self.sellers = sellers
        self.date_first_available = date_first_available
        self.buy_box_owner = buy_box_owner
        self.rating = rating
        self.dimensions = dimensions
        self.product_tier = product_tier
        self.weight_in_lbs = weight_in_lbs


class online_sale_data_uploader:
    def __init__(self):
        self.conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(db_host, db_name, db_username,
                                                                                  db_password)
        self.conn = psycopg2.connect(self.conn_string)
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def list_input_files(self, input_dir):
        for filename in os.listdir(input_dir):
            if os.path.isfile(os.path.join(input_dir, filename)):
                self.read_csv_input(os.path.join(input_dir, filename))

    def standardize_by_removing_lead_dollar_and_comma(self, est_monthly_revenue):
        no_lead_dollar_sign = est_monthly_revenue.replace("$", "")
        no_lead_compare_sign = no_lead_dollar_sign.replace("<", "")
        return no_lead_compare_sign.replace(",", "").strip()

    def standardize_by_removing_comma(self, est_monthly_sales):
        return est_monthly_sales.replace("<", "").replace(",", "").strip()

    def standardize_by_removing_lead_dollar(self, prices_fees_nets):
        return prices_fees_nets.replace("$", "").replace(",", "").strip()

    def standardize_by_removing_comment_sign(self, product_name):
        return product_name.replace('\'', '\'\'')

    def standardize_by_removing_unit(self, weight):
        splits = weight.split(" ")
        return splits[0]

    def standardize_input_data(self, current_item):
        current_item.product_name = self.standardize_by_removing_comment_sign(current_item.product_name)
        current_item.brand = self.standardize_by_removing_comment_sign(current_item.brand)
        current_item.est_monthly_revenue = self.standardize_by_removing_lead_dollar_and_comma(
            current_item.est_monthly_revenue)
        current_item.est_monthly_sales = self.standardize_by_removing_comma(current_item.est_monthly_sales)
        current_item.price = self.standardize_by_removing_lead_dollar(current_item.price)
        current_item.fees = self.standardize_by_removing_lead_dollar(current_item.fees)
        current_item.net = self.standardize_by_removing_lead_dollar(current_item.net)
        current_item.rank = self.standardize_by_removing_lead_dollar(current_item.rank)
        current_item.buy_box_owner = self.standardize_by_removing_comment_sign(current_item.buy_box_owner)
        current_item.weight_in_lbs = self.standardize_by_removing_unit(current_item.weight_in_lbs)

    def persist_to_database(self, current_item):
        query_string = "INSERT INTO online_sale_table(asin, product_name, brand, category, est_monthly_revenue, " \
                       "est_monthly_sales, price, fees, net, rank, reviews, lqs, sellers, date_first_available, " \
                       "buy_box_owner, rating, dimensions, product_tier, weight_in_lbs) VALUES ('{}', '{}', '{}', " \
                       "'{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}', '{}', {}, '{}', '{}', {})" \
            .format(current_item.asin, current_item.product_name, current_item.brand, current_item.category,
                    current_item.est_monthly_revenue, current_item.est_monthly_sales, current_item.price,
                    current_item.fees, current_item.net, current_item.rank, current_item.reviews, current_item.lqs,
                    current_item.sellers, current_item.date_first_available, current_item.buy_box_owner,
                    current_item.rating, current_item.dimensions, current_item.product_tier, current_item.weight_in_lbs)
        print(query_string)
        self.cursor.execute(query_string)
        self.conn.commit()

    def read_csv_input(self, input_file_path):
        with open(input_file_path, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                if len(row) < 15 or row[0].startswith("ASIN"):
                    continue

                for i in range(len(row)):
                    if row[i] == "N.A." or row[i] == '--':
                        row[i] = '0'

                current_item = online_sale_item(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8],
                                                row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16],
                                                row[17], row[18])
                self.standardize_input_data(current_item)
                self.persist_to_database(current_item)


if __name__ == '__main__':
    uploader = online_sale_data_uploader()
    uploader.__init__()
    uploader.list_input_files(input_path)
