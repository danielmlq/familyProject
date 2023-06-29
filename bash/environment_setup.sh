#!/bin/bash

config_file=$1
if [ -z $config_file ]; then
    echo "Need provide config file."
    exit 99
fi

source $config_file

function create_stock_raw() {
    create_stock_raw_sequence_id="create sequence stock_raw_sequence_id_seq"
    create_stock_raw_table="create table if not exists stock_raw (
                                        raw_id Bigint DEFAULT nextval('stock_raw_sequence_id_seq'),
                                        stock_symbol Varchar(50) NOT NULL,
                                        raw_date Date,
                                        open Float DEFAULT 0,
                                        high Float DEFAULT 0,
                                        low Float DEFAULT 0,
                                        close Float DEFAULT 0,
                                        adjust_close Float DEFAULT 0,
                                        volume Double Precision DEFAULT 0,
                                        change_percentage Float DEFAULT 0,
                                        go_up Boolean DEFAULT False,
                                        PRIMARY KEY (raw_id)
                                        )"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_raw_sequence_id"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_raw_table"
}

function create_stock_analysis() {
    create_stock_analysis_sequence_id="create sequence stock_analysis_sequence_id_seq"
    create_stock_analysis_table="create table if not exists stock_analysis (
                                        analysis_id Bigint DEFAULT nextval('stock_analysis_sequence_id_seq'),
                                        stock_symbol Varchar(50) NOT NULL,
                                        start_date Date,
                                        end_date Date,
                                        go_up Boolean DEFAULT False,
                                        total_number_of_days Integer DEFAULT 0,
                                        total_percentage Float DEFAULT 0,
                                        PRIMARY KEY (analysis_id)
                                        )"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_raw_sequence_id"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_raw_table"
}

function create_stock_statistics() {
    create_stock_analysis_sequence_id="create sequence stock_statistics_sequence_id_seq"
    create_stock_analysis_table="create table if not exists stock_statistics (
                                        statistics_id Bigint DEFAULT nextval('stock_statistics_sequence_id_seq'),
                                        stock_symbol Varchar(50) NOT NULL,
                                        statistics_date Date,
                                        president Varchar(500) NOT NULL,
                                        max_days_go_up Float DEFAULT 0,
                                        max_percentage_go_up Float DEFAULT 0,
                                        avg_days_go_up Float DEFAULT 0,
                                        avg_percentage_go_up Float DEFAULT 0,
                                        max_days_go_down Float DEFAULT 0,
                                        max_percentage_go_down Float DEFAULT 0,
                                        avg_days_go_down Float DEFAULT 0,
                                        avg_percentage_go_down Float DEFAULT 0,
                                        PRIMARY KEY (statistics_id)
                                        )"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_raw_sequence_id"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_raw_table"
}

create_stock_raw
create_stock_analysis
create_stock_statistics




#drop table stock_raw;
#drop sequence stock_raw_sequence_id_seq;
#drop table stock_analysis;
#drop sequence stock_analysis_sequence_id_seq;
#drop table stock_statistics;
#drop sequence stock_statistics_sequence_id_seq;