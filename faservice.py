#-------------------------------------------------------------------------------
# Name:        fa_service
# Purpose:
# Author:      Chaus
# Created:     28.10.2020
#-------------------------------------------------------------------------------import os

from configparser import ConfigParser
from falogging import log
import os, sys, re
import requests
import psycopg2

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
        log("Ini-file %s not found!.." %(inifile))
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

            log('Parameter readed:%s'%(key))
        except IOError as e:
            log('Error getting statistic for region:$s'%e)

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

    cursor.close
    conn.close

    return val_list

def get_path(root_path,folder):
    log("Creating folder %s..." %folder)
    base_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(base_path, root_path)
    result_path = os.path.join(result_path, folder)
    if not os.path.exists(result_path):
        try:
            os.mkdir(result_path)
            log("Created %s" %result_path)
        except OSError:
            log("Unable to create %s" %result_path)
    return result_path

def str_to_lst(param_str):
    param_lst = re.sub(r'[\'\"]\s*,\s*[\'\"]','\',\'', param_str.strip('\'\"[]')).split("\',\'")
    return param_lst

#Создание геоиндексов для таблиц из списка outlines
def index_all_region(conn,cursor,outlines):

    log("Creating indexes for %s..." %outlines)

    [dbserver,dbport,dbname,dbuser,dbpass] = get_db_config("db", ["dbserver","dbport","dbname", "dbuser", "dbpass"])

    conn = psycopg2.connect(host=dbserver, port=dbport, dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()

    for outline in outlines:
        try:
            cursor.execute('CREATE INDEX %s_idx ON %s USING GIST (geog)'%(outline,outline))
            conn.commit()
        except IOError as e:
            log('Error indexing geometry $s' % e)

    cursor.close
    conn.close

def send_to_telegram(url, chat, text):
    params = {'chat_id': chat, 'text': text}
    response = requests.post(url + 'sendMessage', data=params)
    if response.status_code != 200:
        raise Exception("post_text error: %s" %response.status_code)
    return response

def send_doc_to_telegram(url, chat, file):
    post_data = {'chat_id': chat}
    post_file = {'document': file}
    response = requests.post(url + 'sendDocument', data=post_data, files = post_file)
    if response.status_code != 200:
        raise Exception("post_text error: %s" %response)
    return response

# Сохраняем созданную таблицу в kml-файл для последующей отправки подписчикам
def write_to_kml(dst_file,whom):

    [dbserver,dbport,dbname,dbuser,dbpass] = get_db_config("db", ["dbserver","dbport","dbname", "dbuser", "dbpass"])

    subs_tab = 'for_s%s' %str(whom)
    log("Writting data from %(s)s table to kml-file: %(f)s..." %{'s':subs_tab, 'f':dst_file})
    if os.path.isfile(dst_file):
        os.remove(dst_file)
        log('Owerwrite kml %s...'%(dst_file))
    else:
        log('Create new kml %s...'%(dst_file))
    command = """ogr2ogr -f "KML" %(d)s PG:"host=%(h)s user=%(u)s dbname=%(b)s password=%(w)s port=%(p)s" %(s)s"""%{'d':dst_file,'s':subs_tab,'h':dbserver,'u':dbuser,'b':dbname,'w':dbpass,'p':dbport}
    os.system(command)
    log('Done.')

def get_cursor():

    [dbserver,dbport,dbname,dbuser,dbpass] = get_db_config("db", ["dbserver","dbport","dbname", "dbuser", "dbpass"])

    conn = psycopg2.connect(host=dbserver, port=dbport, dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()
    return conn, cursor

def close_conn(conn, cursor):
    conn.commit()
    cursor.close
    conn.close
