#!/bin/bash

config_file=$1
if [ -z $config_file ]; then
    echo "Need provide config file."
    exit 99
fi

source $config_file

function create_stock_raw() {
    create_stock_raw_sequence_id="create sequence mau_stock_raw_sequence_id_seq"
    create_stock_raw_table="create table if not exists mau_stock_raw (
                                        raw_id Bigint DEFAULT nextval('mau_stock_raw_sequence_id_seq'),
                                        raw_date Date,
                                        day_index_of_week smallint,
                                        day_of_week Varchar(50),
                                        symbol Varchar(50) NOT NULL,
                                        open Float DEFAULT 0,
                                        high Float DEFAULT 0,
                                        low Float DEFAULT 0,
                                        close Float DEFAULT 0,
                                        adjust_close Float DEFAULT 0,
                                        volume Double Precision DEFAULT 0,
                                        difference_value Float DEFAULT 0,
                                        change_percentage Float DEFAULT 0,
                                        go_up Boolean DEFAULT False,
                                        price_range Varchar(200),
                                        PRIMARY KEY (raw_id)
                                        )"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_raw_sequence_id"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_raw_table"
}

function create_stock_analysis() {
    create_stock_analysis_sequence_id="create sequence mau_stock_analysis_sequence_id_seq"
    create_stock_analysis_table="create table if not exists mau_stock_analysis (
                                        analysis_id Bigint DEFAULT nextval('mau_stock_analysis_sequence_id_seq'),
                                        stock_symbol Varchar(50) NOT NULL,
                                        start_date Date,
                                        end_date Date,
                                        go_up Boolean DEFAULT False,
                                        total_number_of_days Integer DEFAULT 0,
                                        total_percentage Float DEFAULT 0,
                                        avg_daily_percentage Float DEFAULT 0,
                                        price_range Varchar(200),
                                        PRIMARY KEY (analysis_id)
                                        )"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_raw_sequence_id"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_raw_table"
}

function create_stock_weekly_report() {
    create_stock_weekly_report_sequence_id="create sequence mau_stock_weekly_report_sequence_id_seq"
    create_stock_weekly_report_table="create table if not exists mau_stock_weekly_report (
                                        weekly_report_id Bigint DEFAULT nextval('mau_stock_weekly_report_sequence_id_seq'),
                                        stock_symbol Varchar(50) NOT NULL,
                                        start_date Date,
                                        end_date Date,
                                        total_number_of_days Integer DEFAULT 0,
                                        avg_open Float DEFAULT 0,
                                        avg_close Float DEFAULT 0,
                                        difference_value Float DEFAULT 0,
                                        total_percentage Float DEFAULT 0,
                                        go_up Boolean DEFAULT False,
                                        price_range Varchar(200),
                                        end_day_index_of_week smallint,
                                        PRIMARY KEY (weekly_report_id)
                                        )"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_weekly_report_sequence_id"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_weekly_report_table"
}

function create_stock_monthly_report() {
    create_stock_monthly_report_sequence_id="create sequence mau_stock_monthly_report_sequence_id_seq"
    create_stock_monthly_report_table="create table if not exists mau_stock_monthly_report (
                                        monthly_report_id Bigint DEFAULT nextval('mau_stock_monthly_report_sequence_id_seq'),
                                        report_year smallint,
                                        report_month smallint,
                                        stock_symbol Varchar(50) NOT NULL,
                                        start_date Date,
                                        end_date Date,
                                        total_number_of_days Integer DEFAULT 0,
                                        avg_open Float DEFAULT 0,
                                        avg_close Float DEFAULT 0,
                                        difference_value Float DEFAULT 0,
                                        total_percentage Float DEFAULT 0,
                                        go_up Boolean DEFAULT False,
                                        price_range Varchar(200),
                                        PRIMARY KEY (monthly_report_id)
                                        )"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_weekly_report_sequence_id"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_weekly_report_table"
}

function create_stock_quarterly_report() {
    create_stock_quarterly_report_sequence_id="create sequence mau_stock_quarterly_report_sequence_id_seq"
    create_stock_quarterly_report_table="create table if not exists mau_stock_quarterly_report (
                                        quarterly_report_id Bigint DEFAULT nextval('mau_stock_quarterly_report_sequence_id_seq'),
                                        report_year smallint,
                                        report_quarter smallint,
                                        stock_symbol Varchar(50) NOT NULL,
                                        start_date Date,
                                        end_date Date,
                                        total_number_of_days Integer DEFAULT 0,
                                        avg_open Float DEFAULT 0,
                                        avg_close Float DEFAULT 0,
                                        difference_value Float DEFAULT 0,
                                        total_percentage Float DEFAULT 0,
                                        go_up Boolean DEFAULT False,
                                        price_range Varchar(200),
                                        PRIMARY KEY (quarterly_report_id)
                                        )"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_quarterly_report_sequence_id"
    psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -c "$create_stock_quarterly_report_table"
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
create_stock_weekly_report
create_stock_monthly_report
#create_stock_statistics




#drop table mau_stock_raw;
#drop sequence mau_stock_raw_sequence_id_seq;
#drop table mau_stock_analysis;
#drop sequence mau_stock_analysis_sequence_id_seq;
#drop table mau_stock_weekly_report;
#drop sequence mau_stock_weekly_report_sequence_id_seq;
#drop table mau_stock_monthly_report;
#drop sequence mau_stock_monthly_report_sequence_id_seq;
#drop table stock_statistics;
#drop sequence stock_statistics_sequence_id_seq;

