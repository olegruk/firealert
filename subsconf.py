#-------------------------------------------------------------------------------
# Name:        subscribers_config
# Purpose:
# Author:      Chaus
# Created:     15.10.2020
#-------------------------------------------------------------------------------

from faservice import get_config, get_cursor, close_conn

[subs_table] = get_config("tables", ['subs_tab'])
[conf_list, conf_desc] = get_config('subs', ['conf_list', 'conf_desc'])

def add_tlg_user(telegram_id):
    conn, cursor = get_cursor()
    cursor.execute("SELECT subs_id FROM %(s)s WHERE telegramm = '%(t)s'" %{'s':subs_table,'t':telegram_id})
    if len(cursor.fetchall()) == 0:
        cursor.execute("INSERT INTO %(s)s (telegramm) VALUES (%(i)s)" %{'s':subs_table,'i':telegram_id})
        res = True
    else:
        res = False
    close_conn(conn, cursor)
    return res

def set_teleg_stat(telegram_id):
    conn, cursor = get_cursor()
    cursor.execute("UPDATE %(s)s SET teleg_stat = TRUE WHERE telegramm = '%(i)s'" %{'s':subs_table,'i':telegram_id})
    close_conn(conn, cursor)

def unset_teleg_stat(telegram_id):
    conn, cursor = get_cursor()
    cursor.execute("UPDATE %(s)s SET teleg_stat = FALSE WHERE telegramm = '%(i)s'" %{'s':subs_table,'i':telegram_id})
    close_conn(conn, cursor)

def set_teleg_point(telegram_id):
    conn, cursor = get_cursor()
    cursor.execute("UPDATE %(s)s SET teleg_point = TRUE WHERE telegramm = '%(i)s'" %{'s':subs_table,'i':telegram_id})
    close_conn(conn, cursor)

def unset_teleg_point(telegram_id):
    conn, cursor = get_cursor()
    cursor.execute("UPDATE %(s)s SET teleg_point = FALSE WHERE telegramm = '%(i)s'" %{'s':subs_table,'i':telegram_id})
    close_conn(conn, cursor)

def set_active(telegram_id):
    conn, cursor = get_cursor()
    cursor.execute("UPDATE %(s)s SET active = TRUE WHERE telegramm = '%(i)s'" %{'s':subs_table,'i':telegram_id})
    close_conn(conn, cursor)

def unset_active(telegram_id):
    conn, cursor = get_cursor()
    cursor.execute("UPDATE %(s)s SET active = FALSE WHERE telegramm = '%(i)s'" %{'s':subs_table,'i':telegram_id})
    close_conn(conn, cursor)

def add_new_email(telegram_id, email):
    conn, cursor = get_cursor()
    cursor.execute("UPDATE %(s)s SET email = email || ', %(m)s' WHERE email IS NOT NULL and telegramm = '%(i)s'" %{'s':subs_table,'m':email,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET email = '%(m)s' WHERE email IS NULL and telegramm = '%(i)s'" %{'s':subs_table,'m':email,'i':telegram_id})
    cursor.execute("SELECT email FROM %(s)s WHERE telegramm = '%(i)s'" %{'s':subs_table, 'i':telegram_id})
    maillist = cursor.fetchone()
    close_conn(conn, cursor)
    return maillist

def remove_email(telegram_id, email):
    conn, cursor = get_cursor()
    cursor.execute("UPDATE %(s)s SET email = replace(email, '%(m)s, ', '') WHERE telegramm = '%(i)s'" %{'s':subs_table,'m':email,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET email = replace(email, ', %(m)s', '') WHERE telegramm = '%(i)s'" %{'s':subs_table,'m':email,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET email = replace(email, '%(m)s', '') WHERE telegramm = '%(i)s'" %{'s':subs_table,'m':email,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET email = NULL WHERE email = '' and telegramm = '%(i)s'" %{'s':subs_table,'m':email,'i':telegram_id})
    cursor.execute("SELECT email FROM %(s)s WHERE telegramm = '%(i)s'" %{'s':subs_table, 'i':telegram_id})
    maillist = cursor.fetchone()
    close_conn(conn, cursor)
    return maillist

def show_maillist(telegram_id):
    conn, cursor = get_cursor()
    cursor.execute("SELECT email FROM %(s)s WHERE telegramm = '%(i)s'" %{'s':subs_table, 'i':telegram_id})
    maillist = cursor.fetchone()
    close_conn(conn, cursor)
    return maillist

def add_new_region(telegram_id, region):
    conn, cursor = get_cursor()
    cursor.execute("UPDATE %(s)s SET regions = replace(regions, ')', ', ''%(r)s'')') WHERE regions IS NOT NULL and telegramm = '%(i)s'" %{'s':subs_table,'r':region,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET regions = '(''%(r)s'')' WHERE regions IS NULL and telegramm = '%(i)s'" %{'s':subs_table,'r':region,'i':telegram_id})
    cursor.execute("SELECT regions FROM %(s)s WHERE telegramm = '%(i)s'" %{'s':subs_table, 'i':telegram_id})
    reglist = cursor.fetchone()
    close_conn(conn, cursor)
    return reglist

def remove_region(telegram_id, region):
    conn, cursor = get_cursor()
    cursor.execute("UPDATE %(s)s SET regions = replace(regions, '''%(r)s'', ', '') WHERE telegramm = '%(i)s'" %{'s':subs_table,'r':region,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET regions = replace(regions, ', ''%(r)s''', '') WHERE telegramm = '%(i)s'" %{'s':subs_table,'r':region,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET regions = replace(regions, '''%(r)s''', '') WHERE telegramm = '%(i)s'" %{'s':subs_table,'r':region,'i':telegram_id})
    cursor.execute("UPDATE %(s)s SET regions = NULL WHERE regions = '()' and telegramm = '%(i)s'" %{'s':subs_table,'r':region,'i':telegram_id})
    cursor.execute("SELECT regions FROM %(s)s WHERE telegramm = '%(i)s'" %{'s':subs_table, 'i':telegram_id})
    reglist = cursor.fetchone()
    close_conn(conn, cursor)
    return reglist

def show_reglist(telegram_id):
    conn, cursor = get_cursor()
    cursor.execute("SELECT regions FROM %(s)s WHERE telegramm = '%(i)s'" %{'s':subs_table, 'i':telegram_id})
    reglist = cursor.fetchone()
    close_conn(conn, cursor)
    return reglist

def list_reglist():
    conn, cursor = get_cursor()
    cursor.execute("SELECT DISTINCT ON (region) region FROM reg_russia")
    reglist = cursor.fetchall()
    print(reglist)
    close_conn(conn, cursor)
    msg = ''
    for elem in reglist[0:-1]:
        msg = msg + str(elem)[2:-3] + '\n'
    print(msg)
    return msg

def show_conf(telegram_id):
    conn, cursor = get_cursor()
    cursor.execute("SELECT * FROM %(s)s WHERE telegramm = '%(i)s'" %{'s':subs_table, 'i':telegram_id})
    conf = cursor.fetchone()
    close_conn(conn, cursor)
    return conf

