#-------------------------------------------------------------------------------
# Name:        check_alerts
# Purpose:
# Author:      Chaus
# Created:     24.12.2020
#-------------------------------------------------------------------------------

from datetime import datetime, timedelta
from falogging import start_logging, stop_logging
from faservice import get_config
from requester import new_alerts

def check_stat_job():
    start_logging('check_alerts.py')
    
    yesterday = datetime.now() - timedelta(days=1)
    date=yesterday.strftime('%Y-%m-%d')

    [alerts_period] = get_config("alerts", ["period"])
    new_alerts(alerts_period, date)
    print(date) 

    stop_logging('check_alerts.py')

#main
if __name__ == "__main__":
    check_stat_job()
