#!/bin/bash

done_today="False"

while true
do
    echo "current_time: `date +%T`"
    current_hr=`date +%H`
    if [ $current_hr -gt 20 -a $current_hr -le 21 ]; then
        echo "current_hr is between 20 and 21. Running job now"
        sh /Users/danielmeng/Downloads/familyProject/bash/mau_stock_analysis_starter.sh /Users/danielmeng/Downloads/familyProject/config/mau_local.cfg
    elif [ $current_hr -lt 20 ]; then
        echo "current_hr is earlier than 20"
    else
        echo "current_hr is later than 21"
    fi

    sleep 3600

done