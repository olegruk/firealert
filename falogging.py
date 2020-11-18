#-------------------------------------------------------------------------------
# Name:        logging unit
# Purpose:
# Author:      Chaus
# Created:     09.04.2020
#-------------------------------------------------------------------------------

import os, time
import logging

currtime = time.localtime()
date=time.strftime('%Y-%m-%d',currtime)
logfile = "firealert_%s.log" %date
base_path = os.path.dirname(os.path.abspath(__file__))
result_path = os.path.join(base_path, 'log')
if not os.path.exists(result_path):
    try:
        os.mkdir(result_path)
        log("Created %s" % result_path)
    except OSError:
        log("Unable to create %s" % result_path)
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