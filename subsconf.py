#-------------------------------------------------------------------------------
# Name:        subscribers_config
# Purpose:
# Author:      Chaus
# Created:     15.10.2020
#-------------------------------------------------------------------------------

import os, sys
import psycopg2
import falogging, faconfig

conf_list = ['subs_id',
            'subs_name',
            'active',
            'regions',
            'email',
            'telegramm',
            'email_stat',
            'teleg_stat',
            'email_point',
            'teleg_point',
            'stat_period',
            'point_period',
            'crit_or_fire',
            'critical',
            'peatfire',
            'email_first_time',
            'email_period',
            'email_times',
            'teleg_first_time',
            'teleg_period',
            'teleg_times',
            'vip_zones']
conf_desc = ['Идентификатор подписчика: ',
            'Имя подписчика :',
            'Подписка активна :',
            'Список регионов :',
            'Список рассылки :',
            'Telegram_ID :',
            'Статистику по почте :',
            'Статистику в телеграм :',
            'Точки по почте :',
            'Точки в телеграм :',
            'Период, за который смотрим статистику :',
            'Период за который смотрим точки :',
            'Фильтр по критичности или горимости :',
            'Порог критичности :',
            'Порог горимости :',
            'Время первой почтовой рассылки :',
            'Периодичность почтовой рассылки :',
            'Времена почтовой рассылки :',
            'Время первой телеграм-рассылки :',
            'Периодичность телеграм-рассылки :',
            'Времена телеграм-рассылки :',
            'Контролировать VIP-зоны :']

def get_cursor():
    [dbserver,dbport,dbname,dbuser,dbpass] = faconfig.get_db_config("db", ["dbserver","dbport","dbname", "dbuser", "dbpass"])
    subs_table = 'subscribers'
    conn = psycopg2.connect(host=dbserver, port=dbport, dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()
    return conn, cursor, subs_table

def close_conn(conn, cursor):
    conn.commit()
    cursor.close
    conn.close

def add_tlg_user(telegram_id):
    conn, cursor, subs_table = get_cursor()
    cursor.execute("INSERT INTO %(s)s (telegramm) VALUES (%(i)s)" %{'s':subs_table,'i':telegram_id})
    close_conn(conn, cursor)

def set_teleg_stat(telegram_id):
    conn, cursor, subs_table = get_cursor()
    cursor.execute("UPDATE %(s)s SET teleg_stat = TRUE WHERE telegramm = '%(i)s'" %{'s':subs_table,'i':telegram_id})
    close_conn(conn, cursor)

def unset_teleg_stat(telegram_id):
    conn, cursor, subs_table = get_cursor()
    cursor.execute("UPDATE %(s)s SET teleg_stat = FALSE WHERE telegramm = '%(i)s'" %{'s':subs_table,'i':telegram_id})
    close_conn(conn, cursor)

def set_teleg_point(telegram_id):
    conn, cursor, subs_table = get_cursor()
    cursor.execute("UPDATE %(s)s SET teleg_point = TRUE WHERE telegramm = '%(i)s'" %{'s':subs_table,'i':telegram_id})
    close_conn(conn, cursor)

def unset_teleg_point(telegram_id):
    conn, cursor, subs_table = get_cursor()
    cursor.execute("UPDATE %(s)s SET teleg_point = FALSE WHERE telegramm = '%(i)s'" %{'s':subs_table,'i':telegram_id})
    close_conn(conn, cursor)

def set_active(telegram_id):
    conn, cursor, subs_table = get_cursor()
    cursor.execute("UPDATE %(s)s SET active = TRUE WHERE telegramm = '%(i)s'" %{'s':subs_table,'i':telegram_id})
    close_conn(conn, cursor)

def unset_active(telegram_id):
    conn, cursor, subs_table = get_cursor()
    cursor.execute("UPDATE %(s)s SET active = FALSE WHERE telegramm = '%(i)s'" %{'s':subs_table,'i':telegram_id})
    close_conn(conn, cursor)

def add_new_email(telegram_id, email):
    conn, cursor, subs_table = get_cursor()
    cursor.execute("UPDATE %(s)s SET email = email || ', %(m)s' WHERE email IS NOT NULL and telegramm = '%(i)s'" %{'s':subs_table,'m':email,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET email = '%(m)s' WHERE email IS NULL and telegramm = '%(i)s'" %{'s':subs_table,'m':email,'i':telegram_id})
    cursor.execute("SELECT email FROM %(s)s WHERE telegramm = '%(i)s'" %{'s':subs_table, 'i':telegram_id})
    maillist = cursor.fetchone()
    close_conn(conn, cursor)
    return maillist

def remove_email(telegram_id, email):
    conn, cursor, subs_table = get_cursor()
    cursor.execute("UPDATE %(s)s SET email = replace(email, '%(m)s, ', '') WHERE telegramm = '%(i)s'" %{'s':subs_table,'m':email,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET email = replace(email, ', %(m)s', '') WHERE telegramm = '%(i)s'" %{'s':subs_table,'m':email,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET email = replace(email, '%(m)s', '') WHERE telegramm = '%(i)s'" %{'s':subs_table,'m':email,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET email = NULL WHERE email = '' and telegramm = '%(i)s'" %{'s':subs_table,'m':email,'i':telegram_id})
    cursor.execute("SELECT email FROM %(s)s WHERE telegramm = '%(i)s'" %{'s':subs_table, 'i':telegram_id})
    maillist = cursor.fetchone()
    close_conn(conn, cursor)
    return maillist

def show_maillist(telegram_id):
    conn, cursor, subs_table = get_cursor()
    cursor.execute("SELECT email FROM %(s)s WHERE telegramm = '%(i)s'" %{'s':subs_table, 'i':telegram_id})
    maillist = cursor.fetchone()
    close_conn(conn, cursor)
    return maillist

def add_new_region(telegram_id, region):
    conn, cursor, subs_table = get_cursor()
    cursor.execute("UPDATE %(s)s SET regions = replace(regions, ')', ', ''%(r)s'')') WHERE regions IS NOT NULL and telegramm = '%(i)s'" %{'s':subs_table,'r':region,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET regions = '(''%(r)s'')' WHERE regions IS NULL and telegramm = '%(i)s'" %{'s':subs_table,'r':region,'i':telegram_id})
    cursor.execute("SELECT regions FROM %(s)s WHERE telegramm = '%(i)s'" %{'s':subs_table, 'i':telegram_id})
    reglist = cursor.fetchone()
    close_conn(conn, cursor)
    return reglist

def remove_region(telegram_id, region):
    conn, cursor, subs_table = get_cursor()
    cursor.execute("UPDATE %(s)s SET regions = replace(regions, '''%(r)s'', ', '') WHERE telegramm = '%(i)s'" %{'s':subs_table,'r':region,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET regions = replace(regions, ', ''%(r)s''', '') WHERE telegramm = '%(i)s'" %{'s':subs_table,'r':region,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET regions = replace(regions, '''%(r)s''', '') WHERE telegramm = '%(i)s'" %{'s':subs_table,'r':region,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET regions = NULL WHERE regions = '()' and telegramm = '%(i)s'" %{'s':subs_table,'r':region,'i':telegram_id})
    cursor.execute("SELECT regions FROM %(s)s WHERE telegramm = '%(i)s'" %{'s':subs_table, 'i':telegram_id})
    reglist = cursor.fetchone()
    close_conn(conn, cursor)
    return reglist

def show_reglist(telegram_id):
    conn, cursor, subs_table = get_cursor()
    cursor.execute("SELECT regions FROM %(s)s WHERE telegramm = '%(i)s'" %{'s':subs_table, 'i':telegram_id})
    reglist = cursor.fetchone()
    close_conn(conn, cursor)
    return reglist

def show_conf(telegram_id):
    conn, cursor, subs_table = get_cursor()
    cursor.execute("SELECT * FROM %(s)s WHERE telegramm = '%(i)s'" %{'s':subs_table, 'i':telegram_id})
    conf = cursor.fetchone()
    close_conn(conn, cursor)
    return conf

