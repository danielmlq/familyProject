#!/bin/bash

config_file=$1
if [ -z $config_file ]; then
    echo "Need provide config file."
    exit 99
fi

source $config_file

# Start of definition of functions
function create_trader_info() {
    create_online_sale_table_sequence_id="create sequence online_sale_table_sequence_id_seq"
    create_online_sale_table="create table if not exists online_sale_table (
                                        online_sale_id Bigint DEFAULT nextval('online_sale_table_sequence_id_seq'),
                                        ASIN Varchar(200),
                                        Product_Name Varchar(2000),
                                        Brand Varchar(200) NOT NULL,
                                        Category Varchar(100),
                                        Est_Monthly_Revenue Float DEFAULT 0,
                                        Est_Monthly_Sales Bigint,
                                        Price Float DEFAULT 0,
                                        Fees Float DEFAULT 0,
                                        Net Float DEFAULT 0,
                                        Rank Bigint,
                                        Reviews Float DEFAULT 0,
                                        LQS Integer DEFAULT 0,
                                        Sellers Integer DEFAULT 0,
                                        Date_First_Available Date,
                                        Buy_Box_Owner Varchar(50),
                                        Rating Float DEFAULT 0,
                                        Dimensions Varchar(100),
                                        Product_Tier Varchar(100),
                                        Weight_In_Lbs Float DEFAULT 0,
                                        PRIMARY KEY (online_sale_id)
                                        )"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_online_sale_table_sequence_id"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_online_sale_table"
}