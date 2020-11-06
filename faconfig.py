#-------------------------------------------------------------------------------
# Name:        fa_config
# Purpose:
# Author:      Chaus
# Created:     09.04.2020
#-------------------------------------------------------------------------------

import os, sys
from configparser import ConfigParser
import psycopg2
import falogging
from faservice import str_to_lst

#Получение параметров из узла "node" ini-файла "inifile"
#Список имен параметров передается в "param_names"
#Возвращаем список значений
def get_db_config(node, param_names):
    inifile = "firealert.ini"
    base_path = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_path, inifile)

    # get the config
    if os.path.exists(config_path):
        cfg = ConfigParser()
        cfg.read(config_path)
    else:
        falogging.log("Ini-file %s not found!.." %(inifile))
        sys.exit(1)

    # extract params
    param = [cfg.get(node, param_name) for param_name in param_names]
    return param

#Получение параметров из таблицы 'parameters' БД
#Префикс параметра передается в "node"
#Список имен параметров передается в "param_names"
#Возвращаем список значений
def get_config(node, param_names):
    [dbserver,dbport,dbname,dbuser,dbpass] = get_db_config("db", ["dbserver","dbport","dbname", "dbuser", "dbpass"])
    param_table = 'parameters'
    conn = psycopg2.connect(host=dbserver, port=dbport, dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()

    if type(param_names) is not list:
        param_names = [param_names]

    val_list = [None]*len(param_names)
    i = 0
    for param_name in param_names:
        key = '%(n)s.%(p)s' %{'n':node, 'p':param_name}

        try:
            cursor.execute("SELECT strval FROM %(t)s WHERE key = '%(k)s'" %{'t':param_table,'k':key})
            result = cursor.fetchone()
            strval = result[0]
            cursor.execute("SELECT intval FROM %(t)s WHERE key = '%(k)s'" %{'t':param_table,'k':key})
            result = cursor.fetchone()
            intval = result[0]
            cursor.execute("SELECT lstval FROM %(t)s WHERE key = '%(k)s'" %{'t':param_table,'k':key})
            result = cursor.fetchone()
            lststr = result[0]
            if lststr != None:
                lstval = str_to_lst(lststr)
            else:
                lstval = None

            falogging.log('Parameter readed:%s'%(key))
        except IOError as e:
            falogging.log('Error getting statistic for region:$s'%e)

        if strval != None:
            val_list[i] = strval
        elif lstval != None:
            val_list[i] = lstval
        elif intval != None:
            val_list[i] = intval
        else:
            val_list[i] = None
        
        i+=1

        if type(param_names) is not list:
            val_list = val_list[0]

    return val_list