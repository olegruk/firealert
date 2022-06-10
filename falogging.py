#-------------------------------------------------------------------------------
# Name:        logging unit
# Purpose:
# Author:      Chaus
# Created:     09.04.2020
#-------------------------------------------------------------------------------

import os, time, re, traceback, requests
import logging
#from faservice import get_config, send_to_telegram

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

def send_to_telegram(url, chat, text):
    params = {'chat_id': chat, 'text': text}
    response = requests.post(url + 'sendMessage', data=params)
    if response.status_code != 200:
        raise Exception("post_text error: %s" %response.status_code)
    return response

#Протоколирование
def log(msg):
    logging.basicConfig(format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        filename=fulllog)
    logging.warning(msg)
    if re.search(r'rror', msg):
        url = 'https://api.telegram.org/bot990586097:AAHQ8uKZ2q_usZLMDPkbUfFfBJ6-8GLvvlk/'
        chat_id = '-1001416479771'
        errmsg = 'Обнаружены ошибки в log-файлах...\n%s'%msg
        send_to_telegram(url, chat_id, errmsg)
        #send_to_telegram(url, chat_id, msg)
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