#-------------------------------------------------------------------------------
# Name:        index_counturs
# Purpose:
#
# Author:      Chaus
#
# Created:     29.03.2019
#-------------------------------------------------------------------------------

import os, time, sys
import logging
import psycopg2
from configparser import ConfigParser

#Список таблиц, для которых создаем геоиндексы
outlines = ['reg_russia','firms_total']
#outlines = ['buf_core','buf_near','buf_middle','buf_far']
#outlines = ['reg_russia','reg_central']

inifile = "firealert.ini"
logfile = 'firealert.log'

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
        log(logfile, "Ini-file %s not found!.." %(inifile))
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
def log(msg):
    logging.basicConfig(format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        filename=logfile)
    logging.warning(msg)
    #print(msg)

#Создание геоиндексов для таблиц регионов и торфослоя
def index_all_region(conn,cursor,outline):

    try:
        cursor.execute('CREATE INDEX %s_idx ON %s USING GIST (geog)'%(outline,outline))
        conn.commit()
    except IOError as e:
        log(logfile, 'Error indexing geometry $s' % e)

#Задачи
def index_counturs_job():
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d',currtime)
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)

    logfile = get_log_file(date)
    log(logfile, '--------------------------------------------------------------------------------')
    log(logfile, 'Process [index_counturs.py] started at %s'%(cdate))

    # extract params from config
    [dbname,dbuser,dbpass] = get_config(inifile, "db", ["dbname", "dbuser", "dbpass"])

    #connecting to database
    conn = psycopg2.connect(dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()


    #Индексируем таблицы с регионами и буферами
    for outline in outlines:
        index_all_region(conn,cursor,outline)

    cursor.close
    conn.close

    currtime = time.localtime()
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
    log(logfile, 'Process [index_counturs.py] stopped at %s'%(cdate))


#main
if __name__ == "__main__":
    index_counturs_job()
