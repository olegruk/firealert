#-------------------------------------------------------------------------------
# Name:        check_alerts
# Purpose:
# Author:      Chaus
# Created:     24.12.2020
#-------------------------------------------------------------------------------

import time
from falogging import start_logging, stop_logging
from faservice import get_config
from requester import new_alerts

def check_stat_job():
    start_logging('check_alerts.py')
    
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d',currtime)

    [alerts_period] = get_config("alerts", ["period"])
    new_alerts(alerts_period, date) 

    stop_logging('check_alerts.py')

#main
if __name__ == "__main__":
    check_stat_job()
