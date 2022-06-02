#-------------------------------------------------------------------------------
# Name:        logging unit
# Purpose:
# Author:      Chaus
# Created:     09.04.2020
#-------------------------------------------------------------------------------

import os, time, re, traceback
import logging

currtime = time.localtime()
date=time.strftime('%Y-%m-%d',currtime)
root_path = traceback.StackSummary.extract(traceback.walk_stack(None))[-1][0]
uname = re.search(r'\w+\.py', root_path)[0][0:-3]
if uname == 'firealert_bot':
    logfile = "firealert_bot.log"
else:
    #logfile = "firealert_%s.log" %date
    logfile = "%(d)s_%(f)s.log" %{'d':date, 'f':uname}
base_path = os.path.dirname(os.path.abspath(__file__))
result_path = os.path.join(base_path, 'log')
if not os.path.exists(result_path):
    try:
        os.mkdir(result_path)
        #log("Created %s" % result_path)
    except OSError:
        #log("Unable to create %s" % result_path)
        pass
fulllog = os.path.join(result_path, logfile)

def get_log_file(date):
    pass
#    return result_path

#Протоколирование
def log(msg):
    logging.basicConfig(format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        filename=fulllog)
    logging.warning(msg)
    #print(msg)

def start_logging(proc):
    currtime = time.localtime()
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
    log('--------------------------------------------------------------------------------')
    log('Process [%(p)s] started at %(d)s'%{'p':proc, 'd':cdate})

def stop_logging(proc):
    currtime = time.localtime()
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
    log('Process [%(p)s] stopped at %(d)s'%{'p':proc, 'd':cdate})