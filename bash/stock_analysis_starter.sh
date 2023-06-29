#!/bin/bash

config_file=$1
if [ -z $config_file ]; then
    echo "Need provide config file."
    exit 99
fi

source $config_file

start_date='2021-01-19'
end_date='2022-07-18'

echo "Generate raw data"
echo "PYTHON3 /Users/danielmeng/Downloads/familyProject/python/stock_raw_generator.py $config_file $start_date $end_date"
PYTHON3 /Users/danielmeng/Downloads/familyProject/python/stock_raw_generator.py $config_file $start_date $end_date

echo "Persist raw data to database"
echo "psql host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -t -c "\copy stock_raw(high, low, open, close, volume, adjust_close, stock_symbol, raw_date, change_percentage, go_up) from '$RAW_OUTPUT_DIR/output.csv' WITH DELIMITER ',' CSV HEADER"
psql "host=$DB_HOST dbname=$DB_NAME user=$DB_USER password=$DB_PWD" -t -c "\copy stock_raw(high, low, open, close, volume, adjust_close, stock_symbol, raw_date, change_percentage, go_up) from '$RAW_OUTPUT_DIR/output.csv' WITH DELIMITER ',' CSV HEADER"
_rc=$?
if [ $_rc -eq 0 ]; then
    echo "Raw data stored to database successfully."
else
    echo "Raw data stored to database failed."
fi

echo "Generate analysis data"
echo "PYTHON3 /Users/danielmeng/Downloads/familyProject/python/stock_analysis_generator.py $config_file"
PYTHON3 /Users/danielmeng/Downloads/familyProject/python/stock_analysis_generator.py $config_file

echo "Generate statistics data"
echo "PYTHON3 /Users/danielmeng/Downloads/familyProject/python/stock_statistics_generator.py $config_file"
PYTHON3 /Users/danielmeng/Downloads/familyProject/python/stock_statistics_generator.py $config_file