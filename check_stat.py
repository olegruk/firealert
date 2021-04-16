#-------------------------------------------------------------------------------
# Name:        check_stat
# Purpose:
# Author:      Chaus
# Created:     06.02.2020
#-------------------------------------------------------------------------------

import time
from falogging import start_logging, stop_logging
from faservice import get_config, send_to_telegram, smf_new_topic
from requester import make_tlg_stat_msg, make_smf_stat_msg

def check_stat_job():
    start_logging('check_stat.py')
    
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d',currtime)
    fdate=time.strftime('%d-%m-%Y',currtime)

    # extract params from config
    [url, chat_id] = get_config("telegramm", ["url", "wrk_chat_id"])
    [smf_url, smf_user, smf_pass] = get_config("smf", ["smf_url", "smf_user", "smf_pass"])
    [period, critical_limit] = get_config("statistic", ["period", "critical_limit"])
    [reg_list_cr] = get_config("reglists", ["cr"])
    [peat_stat_period] = get_config("peats_stat", ["period"])

    #check_peats_stat(reg_list, date)
    msg = make_tlg_stat_msg(reg_list_cr, period, critical_limit)
    smf_msg = make_smf_stat_msg(reg_list_cr, period, critical_limit)

    send_to_telegram(url, chat_id, msg)
    smf_new_topic(smf_url, smf_user, smf_pass, 13.0, fdate, smf_msg)

    stop_logging('check_stat.py')

#main
if __name__ == "__main__":
    check_stat_job()
