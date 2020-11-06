#-------------------------------------------------------------------------------
# Name:        week_selection
# Purpose:
# Author:      Chaus
# Created:     13.04.2019
#-------------------------------------------------------------------------------

import os, time, sys
import logging
import psycopg2
from configparser import ConfigParser

inifile = "firealert.ini"
logfile = "firealert.log"

critical = 1
#Список областей Центрального региона
reg_list = "('Ярославская область','Тверская область','Смоленская область','Рязанская область','Московская область','Москва','Калужская область','Ивановская область','Владимирская область','Брянская область')"

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

def make_week_selection(conn,cursor,src_tab,critical,period1,period2,reg_list,week_tab):
    user = 'db_reader'
    statements = (
    """
	DROP TABLE IF EXISTS %s
	"""%(week_tab),
	"""
	CREATE TABLE %s (
            name VARCHAR(30),
            acq_date VARCHAR(10),
			acq_time VARCHAR(5),
			latitude NUMERIC,
			longitude NUMERIC,
            sat_sensor VARCHAR(5),
            region  VARCHAR(100),
			peat_district VARCHAR(254),
			peat_id VARCHAR(256),
			peat_class SMALLINT,
			peat_fire SMALLINT,
			rating SMALLINT,
            revision SMALLINT,
			critical SMALLINT,
            geog GEOGRAPHY(POINT, 4326)
	)
	"""%(week_tab),
    """
    INSERT INTO %(w)s (acq_date,acq_time,latitude,longitude,sat_sensor,region,rating,critical,revision,peat_id,peat_district,peat_class,peat_fire,geog)
        SELECT
            %(s)s.acq_date,
            %(s)s.acq_time,
            %(s)s.latitude,
            %(s)s.longitude,
            %(s)s.satellite,
            %(s)s.region,
            %(s)s.rating,
            %(s)s.critical,
            %(s)s.revision,
            %(s)s.peat_id,
            %(s)s.peat_district,
            %(s)s.peat_class,
            %(s)s.peat_fire,
            %(s)s.geog
        FROM %(s)s
        WHERE (date_time > TIMESTAMP 'today' - INTERVAL '%(p1)s') AND (date_time <= TIMESTAMP 'today' - INTERVAL '%(p2)s')
        ORDER BY %(s)s.peat_id
    """%{'w':week_tab,'s':src_tab,'p1':period1,'p2':period2,'c':critical,'r':reg_list}, #AND (critical >= %(c)s OR revision >= %(c)s) AND region in %(r)s
    """
	UPDATE %s
		SET name = ''
    """%(week_tab),
    """
	UPDATE %s
		SET sat_sensor = 'VIIRS'
        WHERE sat_sensor = 'N'
    """%(week_tab),
    """
	UPDATE %s
		SET sat_sensor = 'VNOAA'
        WHERE sat_sensor = '1'
    """%(week_tab),
    """
	UPDATE %s
		SET sat_sensor = 'MODIS'
        WHERE (sat_sensor = 'A') OR (sat_sensor = 'T')
    """%(week_tab),
	"""
	GRANT SELECT ON %(t)s TO %(u)s
	"""%{'t': week_tab, 'u': user},
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log(logfile, 'The table created:%s'%(week_tab))
    except IOError as e:
        log(logfile, 'Error creating week tables:$s'%e)

def week_selection_job():
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d',currtime)
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)

    logfile = get_log_file(date)
    log(logfile, '--------------------------------------------------------------------------------')
    log(logfile, 'Process [week_selection.py] started at %s'%(cdate))

    # extract db params from config
    [dbname,dbuser,dbpass] = get_config(inifile, "db", ["dbname", "dbuser", "dbpass"])
    [year_tab] = get_config(inifile, "tables", ["year_tab"])

    #connecting to database
    conn = psycopg2.connect(dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()

    for i in range(7):
        period1 = "%s hours"%(24*(i))
        period2 = "%s hours"%(24*(i-1))
        week_tab = "days_ago_%s"%(i+1)
        if week_tab == "days_ago_0":
            week_tab = "days_today"
        make_week_selection(conn,cursor,year_tab,critical,period1,period2,reg_list,week_tab)

    cursor.close
    conn.close

    currtime = time.localtime()
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
    log(logfile, 'Process [week_selection.py] stopped at %s'%(cdate))

#main
if __name__ == '__main__':
    week_selection_job()
