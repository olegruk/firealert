#-------------------------------------------------------------------------------
# Name:        happy_new_year
# Purpose:
# Author:      Chaus
# Created:     20.12.2019
#-------------------------------------------------------------------------------

import os, time, sys
import logging
import psycopg2
from configparser import ConfigParser

inifile = "firealert.ini"
logfile = "firealert.log"

#Получение параметров из узла "node" ini-файла "inifile"
#Список имен параметров передается в "param_names"
#Возвращаем список значений
def get_config(inifile, node, param_names):
    base_path = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_path, inifile)

    # get the config
    if os.path.exists(config_path):
        cfg = ConfigParser()
        cfg.read(config_path)
    else:
        log(logfile, "ini-file not found!..")
        sys.exit(1)

    # extract params
    param = [cfg.get(node, param_name) for param_name in param_names]
    return param

def get_log_file(date):
    [logfile, log_folder] = get_config(inifile, "path", ["logfile", "log_folder"])
    logfile = "%(l)s_%(d)s.log"%{'l':logfile,'d':date}
    base_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(base_path, log_folder)
    if not os.path.exists(result_path):
        try:
            os.mkdir(result_path)
            log(logfile, "Created %s" % result_path)
        except OSError:
            log(logfile, "Unable to create %s" % result_path)
    result_path = os.path.join(result_path, logfile)
    return result_path

#Протоколирование
def log(logfile, msg):
    logging.basicConfig(format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        filename=logfile)
    logging.warning(msg)
    #print(msg)

def copy_table(conn,cursor,src_tab,now_year):
    dst_tab = src_tab + '_' + str(now_year)
    statements = (
    """
    ALTER TABLE %(s)s RENAME TO %(d)s
    """%{'s': src_tab, 'd': dst_tab},
#   """
#	DROP TABLE IF EXISTS %s
#	"""%(src_tab),
    """
    CREATE TABLE %(s)s AS
        SELECT * FROM %(d)s
        WITH NO DATA
    """%{'s': src_tab, 'd': dst_tab}
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log(logfile, 'The table copied:%s'%(src_tab))
    except IOError as e:
        log(logfile, 'Error copying year table:$s'%e)

def happy_new_year_job():
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d',currtime)
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
    now_year = int(time.strftime('%Y',currtime))-1

    logfile = get_log_file(date)
    log(logfile, '--------------------------------------------------------------------------------')
    log(logfile, 'Process [happy_new_year.py] started at %s'%(cdate))

    # extract db params from config
    [dbname,dbuser,dbpass] = get_config(inifile, "db", ["dbname", "dbuser", "dbpass"])
    [year_tab] = get_config(inifile, "tables", ["year_tab"])

    #connecting to database
    conn = psycopg2.connect(dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()

    copy_table(conn,cursor,year_tab,now_year)

    cursor.close
    conn.close

    currtime = time.localtime()
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
    log(logfile, 'Process [happy_new_year.py] stopped at %s'%(cdate))

#main
if __name__ == '__main__':
    happy_new_year_job()
