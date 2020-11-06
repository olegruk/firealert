#-------------------------------------------------------------------------------
# Name:        make_fire_clusters
# Purpose:
# Author:      Chaus
# Created:     18.04.2019
#-------------------------------------------------------------------------------


import os, time, sys
import logging
import psycopg2
from osgeo import ogr
import yadisk
import posixpath
import smtplib
from configparser import ConfigParser
from email import encoders
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate

#Список подписчиков
point_tab = 'per_season'
clust_tab = 'fire_clusters'
centr_tab = 'fire_center'
peat_tab = 'fire_peats'

#Порог горимости - точки,
#попавшие в контур и буферы торфяника
#с горимостью не ниже этого параметра
critical = {'fire_clusters': 120, 'test': 120}

#Списки регионов
reg_list_cr = "('Ярославская область','Тверская область','Смоленская область','Рязанская область','Московская область','Москва','Калужская область','Ивановская область','Владимирская область','Брянская область')"
reg_list_mo = "('Московская область','Москва','Смоленская область')"
reg_list = {'fire_clusters': reg_list_cr, 'test': reg_list_mo}

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

#Создаем таблицу для выгрузки дешифровщику
def make_table_for(conn,cursor,src_tab,critical,period,reg_list,point_tab,centr_tab,clust_tab,peat_tab,dist,buf):
    user = 'db_reader'
    statements = (
        """
		DROP TABLE IF EXISTS %s
		"""%(point_tab),
		"""
		CREATE TABLE %s (
                name VARCHAR(30),
                description VARCHAR(256),
                acq_date VARCHAR(10),
				acq_time VARCHAR(5),
                sat_sensor VARCHAR(5),
                region  VARCHAR(100),
				rating SMALLINT,
				critical SMALLINT,
				peat_id VARCHAR(256),
				peat_district VARCHAR(254),
				peat_class SMALLINT,
				peat_fire SMALLINT,
                geom GEOMETRY(POINT, 3857)
		)
		"""%(point_tab),
        """
        INSERT INTO %(w)s (name,acq_date,acq_time,sat_sensor,region,rating,critical,peat_id,peat_district,peat_class,peat_fire,geom)
            SELECT
                %(s)s.name,
                %(s)s.acq_date,
                %(s)s.acq_time,
                %(s)s.satellite,
                %(s)s.region,
                %(s)s.rating,
                GREATEST(%(s)s.critical,%(s)s.revision),
                %(s)s.peat_id,
                %(s)s.peat_district,
                %(s)s.peat_class,
                %(s)s.peat_fire,
                ST_Transform(%(s)s.geog::geometry,3857)::geometry
            FROM %(s)s
            WHERE date_time >= NOW() - INTERVAL '%(p)s' AND (critical >= %(c)s OR revision >= %(c)s) AND region in %(r)s
        """%{'w':point_tab,'s':src_tab,'p':period,'c':critical[clust_tab],'r':reg_list[clust_tab]},
        """
		DROP TABLE IF EXISTS %s
		"""%(clust_tab),
        """
        CREATE TABLE %(c)s
            AS SELECT
                peat_id,
                ST_Buffer(ST_ConvexHull(unnest(ST_ClusterWithin(geom, %(d)s))), %(b)s, 'quad_segs=8') AS buffer
            FROM %(s)s
            GROUP BY peat_id
        """%{'c': clust_tab, 's': point_tab, 'd': dist, 'b': buf},
        """
		DROP TABLE IF EXISTS %s
		"""%(centr_tab),
        """
        CREATE TABLE %(c)s
            AS SELECT
                peat_id,
                ST_Centroid(buffer)
            FROM %(s)s
        """%{'c': centr_tab,'s': clust_tab},
       """
		DROP TABLE IF EXISTS %s
		"""%(peat_tab),
        """
        CREATE TABLE %(p)s
            AS SELECT DISTINCT ON (peat_id) *
            FROM %(s)s
        """%{'p': peat_tab, 's': point_tab},
	"""
	GRANT SELECT ON %(t)s TO %(u)s
	"""%{'t': point_tab, 'u': user},
	"""
	GRANT SELECT ON %(t)s TO %(u)s
	"""%{'t': clust_tab, 'u': user},
	"""
	GRANT SELECT ON %(t)s TO %(u)s
	"""%{'t': centr_tab, 'u': user},
	"""
	GRANT SELECT ON %(t)s TO %(u)s
	"""%{'t': peat_tab, 'u': user}
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log(logfile, 'The table created:%s'%(clust_tab))
    except IOError as e:
        log(logfile, 'Error creating subscribers tables:$s'%e)
    cursor.execute("SELECT count(*) FROM %s"%(clust_tab))
    return cursor.fetchone()[0]

def make_fire_clusters_job():
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d',currtime)
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)

    logfile = get_log_file(date)
    log(logfile, '--------------------------------------------------------------------------------')
    log(logfile, 'Process [make_fire_clusters.py] started at %s'%(cdate))

    # extract params from config
    [dbserver,dbport,dbname,dbuser,dbpass] = get_config(inifile, "db", ["dbserver","dbport","dbname", "dbuser", "dbpass"])
    [year_tab] = get_config(inifile, "tables", ["year_tab"])
    [clst_dist,clst_buf,clst_period] = get_config(inifile, "clusters", ["cluster_dist","cluster_buf","cluster_period"])

    #connecting to database
    conn = psycopg2.connect(dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()

    num_points = make_table_for(conn,cursor,year_tab,critical,clst_period,reg_list,point_tab,centr_tab,clust_tab,peat_tab,clst_dist,clst_buf)
    #print('Создано %s записей'%num_points)

    cursor.close
    conn.close

    currtime = time.localtime()
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
    log(logfile, 'Process [make_fire_clusters.py] stopped at %s'%(cdate))

#main
if __name__ == "__main__":
    make_fire_clusters_job()
