#-------------------------------------------------------------------------------
# Name:        check_zones
# Purpose:
# Author:      Chaus
# Created:     06.02.2020
#-------------------------------------------------------------------------------

from faservice import get_config, send_to_telegram
from falogging import start_logging, stop_logging
from requester import check_vip_zones

def check_zones_job():

    start_logging('check_zones.py')
    
    # extract params from config
    [url, chat_id] = get_config("telegramm", ["url", "wrk_chat_id"])
    [period] = get_config('statistic', ['period'])
    [outline] = get_config('tables', ['vip_zones'])

    points_count, zones = check_vip_zones(outline, period)

    if points_count > 0:
        msg = 'Новых точек\r\nв зонах особого внимания: %s\r\n\r\n' %points_count
        for (zone, num_points) in zones:
            msg = msg + '%s - %s\r\n' %(zone, num_points)

        send_to_telegram(url, chat_id, msg)

    stop_logging('check_zones.py')
    
#main
if __name__ == "__main__":
    check_zones_job()
