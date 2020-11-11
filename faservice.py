#-------------------------------------------------------------------------------
# Name:        fa_service
# Purpose:
# Author:      Chaus
# Created:     28.10.2020
#-------------------------------------------------------------------------------import os

from falogging import log
from faconfig import get_db_config
import os, re
import requests
import psycopg2

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

def get_cursor():

    [dbserver,dbport,dbname,dbuser,dbpass] = get_db_config("db", ["dbserver","dbport","dbname", "dbuser", "dbpass"])

    conn = psycopg2.connect(host=dbserver, port=dbport, dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()
    return conn, cursor

def close_conn(conn, cursor):
    conn.commit()
    cursor.close
    conn.close
