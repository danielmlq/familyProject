#!/bin/bash

config_file=$1
if [ -z $config_file ]; then
    echo "Need provide config file."
    exit 99
fi

source $config_file

#start_date="2022-12-15"
#end_date="2022-12-16"
#start_date=$(date -v -1d '+%Y-%m-%d')

start_date=`psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -t -c "select max(raw_date) from mau_stock_raw"`
end_date=$(date +'%Y-%m-%d')

# Check if stock market opens today or not
outputString=$(PYTHON3 /Users/danielmeng/Downloads/familyProject/python/market_open_checker.py $end_date 2>&1)
if [[ $outputString = "Close" ]]; then
    echo "Stock market close on $end_date"
    exit 0
fi

echo "Stock market open on $end_date"

echo "Generate raw data"
echo "PYTHON3 /Users/danielmeng/Downloads/familyProject/python/mau_stock_raw_generator.py $config_file $start_date $end_date"
PYTHON3 /Users/danielmeng/Downloads/familyProject/python/mau_stock_raw_generator.py $config_file $start_date $end_date

echo "Persist raw data to database"
echo "psql host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD -t -c \copy mau_stock_raw(high, low, open, close, volume, adjust_close, symbol, raw_date, day_index_of_week, day_of_week, difference_value, change_percentage, go_up, price_range) from '$RAW_OUTPUT_DIR/output.csv' WITH DELIMITER ',' CSV HEADER"
psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -t -c "\copy mau_stock_raw(high, low, open, close, volume, adjust_close, symbol, raw_date, day_index_of_week, day_of_week, difference_value, change_percentage, go_up, price_range) from '$RAW_OUTPUT_DIR/output.csv' WITH DELIMITER ',' CSV HEADER"
_rc=$?
if [ $_rc -eq 0 ]; then
    echo "Raw data stored to database successfully."
else
    echo "Raw data stored to database failed."
fi

echo "Generate analysis data"
echo "PYTHON3 /Users/danielmeng/Downloads/familyProject/python/mau_stock_analysis_generator.py $config_file"
PYTHON3 /Users/danielmeng/Downloads/familyProject/python/mau_stock_analysis_generator.py $config_file

echo "Generate weekly report data"
echo "PYTHON3 /Users/danielmeng/Downloads/familyProject/python/mau_stock_weekly_report.py $config_file"
PYTHON3 /Users/danielmeng/Downloads/familyProject/python/mau_stock_weekly_report.py $config_file

echo "Generate monthly report data"
echo "PYTHON3 /Users/danielmeng/Downloads/familyProject/python/mau_stock_monthly_report.py $config_file"
PYTHON3 /Users/danielmeng/Downloads/familyProject/python/mau_stock_monthly_report.py $config_file

echo "Generate quarterly report data"
echo "PYTHON3 /Users/danielmeng/Downloads/familyProject/python/mau_stock_quarterly_report.py $config_file"
PYTHON3 /Users/danielmeng/Downloads/familyProject/python/mau_stock_quarterly_report.py $config_file

echo "Exporter top20 volume all information"
echo "PYTHON3 /Users/danielmeng/Downloads/familyProject/python/mau_stock_exporter.py $config_file $end_date"
PYTHON3 /Users/danielmeng/Downloads/familyProject/python/mau_stock_exporter.py $config_file $end_date