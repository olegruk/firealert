#-------------------------------------------------------------------------------
# Name:        daily_masspoint
# Purpose:
# Author:      Chaus
# Created:     23.12.2019
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
#subscribers = ['cr']
#subscribers = ['cr', 'mo']
subscribers = ['kk']
#subscribers = ['test']

#Списки регионов
reg_list_cr = "('Ярославская область','Тверская область','Смоленская область','Рязанская область','Московская область','Москва','Калужская область','Ивановская область','Владимирская область','Брянская область')"
reg_list_mo = "('Московская область','Москва','Смоленская область')"
reg_list_kk = "('Красноярский край')"
reg_list = {
    'cr': reg_list_cr,
    'mo': reg_list_mo,
    'kk': reg_list_kk,
    'test': reg_list_mo}

#e-mail для рассылки
mail_addr = {
    'cr': ['i-semenov83@yandex.ru','dariiak13@gmail.com','nasic@mail.ru','xoma-doma@list.ru','chaus@firevolonter.ru','fly220@mail.ru'],
    'mo': ['chaus@firevolonter.ru'],
    'kk': ['chaus@firevolonter.ru'],
    'test': ['chaus@firevolonter.ru']}

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
def make_table_for(conn,cursor,src_tab,masspoint_count,period,reg_list,point_tab,clust_tab,centr_tab,dist,buf):
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
                sat_sensor VARCHAR(20),
                region  VARCHAR(100),
                geom GEOMETRY(POINT, 3857)
		)
		"""%(point_tab),
        """
        INSERT INTO %(w)s (name,acq_date,acq_time,sat_sensor,region,geom)
            SELECT
                %(s)s.name,
                %(s)s.acq_date,
                %(s)s.acq_time,
                %(s)s.satellite,
                %(s)s.region,
                ST_Transform(%(s)s.geog::geometry,3857)::geometry
            FROM %(s)s
            WHERE "date_time" > TIMESTAMP 'today' - INTERVAL '%(p)s' AND  "date_time" < TIMESTAMP 'today' AND region in %(r)s
        """%{'w':point_tab,'s':src_tab,'p':period,'r':reg_list},
        """
		UPDATE %s
			SET sat_sensor = 'MODIS - Aqua'
            WHERE sat_sensor = 'A'
        """%(point_tab),
        """
		UPDATE %s
			SET sat_sensor = 'MODIS - Terra'
            WHERE sat_sensor = 'T'
        """%(point_tab),
        """
		UPDATE %s
			SET sat_sensor = 'VIIRS - SUOMI NPP'
            WHERE sat_sensor = 'N'
        """%(point_tab),
        """
		UPDATE %s
			SET sat_sensor = 'VIIRS - NOAA'
            WHERE sat_sensor = '1'
        """%(point_tab),
        """
		UPDATE %s
			SET description =
            'Дата: ' || acq_date || '\n' ||
            'Время: ' || acq_time || '\n' ||
            'Сенсор: ' || sat_sensor || '\n' ||
            'Регион: ' || region || '\n'
        """%(point_tab),
        """
		DROP TABLE IF EXISTS %s
		"""%(clust_tab),
        """
        CREATE TABLE %(c)s
            AS SELECT
                ST_NumGeometries(gc) as num_geometries,
                ST_Buffer(ST_ConvexHull(gc), %(b)s, 'quad_segs=8') AS buffer
               FROM
                (SELECT unnest(ST_ClusterWithin(geom, %(d)s)) AS gc
                FROM %(s)s) as result
               WHERE ST_NumGeometries(gc) >= %(m)s
        """%{'c': clust_tab, 's': point_tab, 'd': dist, 'b': buf, 'm': masspoint_count},
        """
		DROP TABLE IF EXISTS %s
		"""%(centr_tab),
        """
        CREATE TABLE %(c)s
            AS SELECT
                 ST_Transform(ST_Centroid(buffer),4326) as geom
            FROM %(s)s
        """%{'c': centr_tab,'s': clust_tab},
        """
        ALTER TABLE %s
            ADD COLUMN center VARCHAR(50)
        """%(centr_tab),
        """
		UPDATE %s
			SET center = ST_AsText(geom)
        """%(centr_tab),
        """
        ALTER TABLE %s
            ADD COLUMN center VARCHAR(50),
            ADD COLUMN description VARCHAR(255)
        """%(clust_tab),
        """
		UPDATE %s
			SET center = ST_AsText(ST_Transform(ST_Centroid(buffer),4326))
        """%(clust_tab),
        """
		UPDATE %s
			SET center = substring(center from 24 for 9) || ', ' || substring(center from 7 for 9)
        """%(clust_tab),
        """
		UPDATE %s
			SET description =
            'Точек в кластере: ' || num_geometries || '\n' ||
            'Центр: ' || center || '\n'
        """%(clust_tab),
    	"""
    	GRANT SELECT ON %(t)s TO %(u)s
        """%{'t': point_tab, 'u': user},
    	"""
    	GRANT SELECT ON %(t)s TO %(u)s
        """%{'t': centr_tab, 'u': user},
#        """
#		DROP TABLE IF EXISTS %s
#		"""%(point_tab),
#        """
        """
        GRANT SELECT ON %(t)s TO %(u)s
        """%{'t': clust_tab, 'u': user}
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log(logfile, 'The table created:%s'%(clust_tab))
    except IOError as e:
        log(logfile, 'Error creating subscribers tables:$s'%e)
    cursor.execute("SELECT count(*) FROM %s"%(point_tab))
    num_of_points = cursor.fetchone()[0]
    cursor.execute("SELECT count(*) FROM %s"%(clust_tab))
    num_of_clusters = cursor.fetchone()[0]
    return num_of_points, num_of_clusters

def write_to_kml(src_tab,dst_file,dbserver,dbport,dbname,dbuser,dbpass):
    if os.path.isfile(dst_file):
        os.remove(dst_file)
        log(logfile, 'Owerwrite kml %s...'%(dst_file))
    else:
        log(logfile, 'Create new kml %s...'%(dst_file))
    command = """ogr2ogr -f "KML" %(d)s PG:"host=%(h)s user=%(u)s dbname=%(b)s password=%(w)s port=%(p)s" %(s)s"""%{'d':dst_file,'s':src_tab,'h':dbserver,'u':dbuser,'b':dbname,'w':dbpass,'p':dbport}
    os.system(command)
    log(logfile, 'Done.')

#Send an email with an attachment
def send_email_with_points(inifile, date, emails, path_to_attach, file_to_attach, num_point):

    # extract server and from_addr from config
    [host,from_addr,user,pwd] = get_config(inifile, "smtp", ["server", "from_addr", "user", "pwd"])

    header = 'Content-Disposition', 'attachment; filename="%s"' % file_to_attach
    subject = "Masspoints for %(d)s - (%(n)s points)"%{'d':date, 'n':num_point}
    body_text = "In the attachment masspoints for last 24 hour.\r\nEmail to dist_mon@firevolonter.ru if you find any errors or inaccuracies."

    # create the message
    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)

    if body_text:
        msg.attach( MIMEText(body_text) )

    msg["To"] = ', '.join(emails)

    attachment = MIMEBase('application', "octet-stream")
    file_with_path = os.path.join(path_to_attach,file_to_attach)
    try:
        with open(file_with_path, "rb") as fh:
            data = fh.read()

        attachment.set_payload( data )
        encoders.encode_base64(attachment)
        attachment.add_header(*header)
        msg.attach(attachment)
    except IOError:
        msg = "Error opening attachment file %s" % file_with_path
        log(logfile, msg)
        sys.exit(1)

    mailserver = smtplib.SMTP(host,587)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.ehlo()
    mailserver.login(user, pwd)
    mailserver.sendmail(from_addr, emails, msg.as_string())
    mailserver.quit()

#Send an email with an attachment
def send_email_with_clusters(inifile, date, emails, path_to_attach, file_to_attach, num_clust):

    # extract server and from_addr from config
    [host,from_addr,user,pwd] = get_config(inifile, "smtp", ["server", "from_addr", "user", "pwd"])

    header = 'Content-Disposition', 'attachment; filename="%s"' % file_to_attach
    subject = "Fireclusters for %(d)s - (%(n)s clusters)"%{'d':date, 'n':num_clust}
    body_text = "In the attachment polygons around masspoint places for last 24 hour.\r\nEmail to dist_mon@firevolonter.ru if you find any errors or inaccuracies."

    # create the message
    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)

    if body_text:
        msg.attach( MIMEText(body_text) )

    msg["To"] = ', '.join(emails)

    attachment = MIMEBase('application', "octet-stream")
    file_with_path = os.path.join(path_to_attach,file_to_attach)
    try:
        with open(file_with_path, "rb") as fh:
            data = fh.read()

        attachment.set_payload( data )
        encoders.encode_base64(attachment)
        attachment.add_header(*header)
        msg.attach(attachment)
    except IOError:
        msg = "Error opening attachment file %s" % file_with_path
        log(logfile, msg)
        sys.exit(1)

    mailserver = smtplib.SMTP(host,587)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.ehlo()
    mailserver.login(user, pwd)
    mailserver.sendmail(from_addr, emails, msg.as_string())
    mailserver.quit()

def check_daily_masspoint_job():
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d',currtime)
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)

    logfile = get_log_file(date)
    log(logfile, '--------------------------------------------------------------------------------')
    log(logfile, 'Process [daily_masspoint.py] started at %s'%(cdate))

    # extract params from config
    [dbserver,dbport,dbname,dbuser,dbpass] = get_config(inifile, "db", ["dbserver","dbport","dbname", "dbuser", "dbpass"])
    [year_tab] = get_config(inifile, "tables", ["year_tab"])
    [masspoint_dist,masspoint_buf,masspoint_period,masspoint_count] = get_config(inifile, "masspoint", ["masspoint_dist","masspoint_buf","masspoint_period","masspoint_count"])
    [data_root,result_folder] = get_config(inifile, "path", ["data_root", "result_folder"])

    #connecting to database
    conn = psycopg2.connect(host=dbserver, port=dbport, dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()

    #Создаем каталоги
    result_dir = get_path(data_root,result_folder)

    for subscriber in subscribers:
        point_tab = 'daily_masspoints_' + subscriber
        clust_tab = 'daily_clusters_' + subscriber
        centr_tab = 'daily_centroids_' + subscriber
        dst_point_file_name = '%(d)s_masspoint_for_%(s)s.kml'%{'d': date, 's': subscriber}
        dst_clust_file_name = '%(d)s_fireclusters_for_%(s)s.kml'%{'d': date, 's': subscriber}
        dst_point_file = os.path.join(result_dir,dst_point_file_name)
        dst_clust_file = os.path.join(result_dir,dst_clust_file_name)
        num_point, num_clust = make_table_for(conn,cursor,year_tab,masspoint_count,masspoint_period,reg_list[subscriber],point_tab,clust_tab,centr_tab,masspoint_dist,masspoint_buf)
        if num_clust > 0:
            write_to_kml(point_tab,dst_point_file,dbserver,dbport,dbname,dbuser,dbpass)
            write_to_kml(clust_tab,dst_clust_file,dbserver,dbport,dbname,dbuser,dbpass)
            send_email_with_points(inifile, date,  mail_addr[subscriber], result_dir, dst_point_file_name, num_point)
            send_email_with_clusters(inifile, date,  mail_addr[subscriber], result_dir, dst_clust_file_name, num_clust)

    cursor.close
    conn.close

    currtime = time.localtime()
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
    log(logfile, 'Process [daily_masspoint.py] stopped at %s'%(cdate))

#main
if __name__ == "__main__":
    check_daily_masspoint_job()
