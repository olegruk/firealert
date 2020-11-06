#-------------------------------------------------------------------------------
# Name:        check_zones
# Purpose:
# Author:      Chaus
# Created:     06.02.2020
#-------------------------------------------------------------------------------


import os, time, sys
import logging
import requests
import psycopg2
from configparser import ConfigParser

period = '24 hours'
#period = '1 week'
#period = '13 month'

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
def log(logfile, msg):
    logging.basicConfig(format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        filename=logfile)
    logging.warning(msg)
    #print(msg)

def get_path(root_path,folder):
    base_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(base_path, root_path)
    result_path = os.path.join(result_path, folder)
    if not os.path.exists(result_path):
        try:
            os.mkdir(result_path)
            log (logfile, "Created %s" % result_path)
        except OSError:
            log (logfile, "Unable to create %s" % result_path)
    return result_path

def send_to_telegram(url, chat, text):
    params = {'chat_id': chat, 'text': text}
    response = requests.post(url + 'sendMessage', data=params)
    if response.status_code != 200:
        raise Exception("post_text error: %s" %response.status_code)
    return response

def check_vip_zones(conn, cursor, year_tab, dst_tab, outline, period):
    log(logfile, "Checking VIP-zones...")
    statements = (
		"""
		DROP TABLE IF EXISTS %s
		"""%(dst_tab),
		"""
		CREATE TABLE %(d)s
			AS SELECT %(s)s.ident, %(o)s.name AS zone_name, %(s)s.geog
				FROM %(s)s, %(o)s
				WHERE (%(s)s.date_time > TIMESTAMP 'today' - INTERVAL '%(p)s') AND (%(s)s.vip IS NULL) AND (ST_Intersects(%(o)s.geog, %(s)s.geog))
		"""%{'d':dst_tab, 's':year_tab, 'o':outline, 'p':period},
        """
    	UPDATE %(y)s
		SET vip = 1
        FROM %(d)s
        WHERE %(d)s.ident = %(y)s.ident
		"""%{'d':dst_tab, 'y':year_tab}
        )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log(logfile, 'The table created:%s'%(dst_tab))
    except IOError as e:
        log(logfile, 'Error intersecting points with region:$s'%e)
    cursor.execute("SELECT count(*) FROM %s"%(dst_tab))
    points_count = cursor.fetchone()[0]
    #cursor.execute("SELECT DISTINCT zone_name FROM %s"%(dst_tab))
    cursor.execute("SELECT zone_name, COUNT(*) FROM %s GROUP BY zone_name"%(dst_tab))
    zones = cursor.fetchall()
    return points_count, zones

def check_zones_job():
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d',currtime)
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)

    logfile = get_log_file(date)
    log(logfile, '--------------------------------------------------------------------------------')
    log(logfile, 'Process [check_zones.py] started at %s'%(cdate))

    # extract params from config
    [dbname,dbuser,dbpass] = get_config(inifile, "db", ["dbname", "dbuser", "dbpass"])
    [year_tab] = get_config(inifile, "tables", ["year_tab"])
    dst_tab = year_tab + '_vip'
    outline = 'vip_zones'

    url = "https://api.telegram.org/bot990586097:AAHfAk360-IEPcgc7hitDSyD7pu9rzt5tbE/"
    #chat_id = "-1001416479771" #@firealert-test
    chat_id = "-1001179749742" #@firealert

    #connecting to database
    conn = psycopg2.connect(dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()

    points_count, zones = check_vip_zones(conn, cursor, year_tab, dst_tab, outline, period)

    if points_count > 0:
        msg = 'Новых точек\r\nв зонах особого внимания: %s\r\n\r\n' %points_count
        for (zone, num_points) in zones:
            msg = msg + '%s - %s\r\n' %(zone, num_points)

        send_to_telegram(url, chat_id, msg)

    cursor.close
    conn.close

    currtime = time.localtime()
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
    log(logfile, 'Process [check_zones.py] stopped at %s'%(cdate))

#main
if __name__ == "__main__":
    check_zones_job()
