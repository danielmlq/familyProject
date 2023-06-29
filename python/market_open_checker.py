import sys
import pandas_market_calendars as mcal

current_dt = sys.argv[1]
nyse = mcal.get_calendar('NYSE')
result = nyse.valid_days(start_date=current_dt, end_date=current_dt).strftime("%Y-%m-%d").tolist()

if len(result) == 0:
    sys.exit("Close")
else:
    sys.exit("Open")