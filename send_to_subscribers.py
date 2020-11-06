#-------------------------------------------------------------------------------
# Name:        send_to_subscribers
# Purpose:
# Author:      Chaus
# Created:     31.03.2019
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

#Список типов подписчиков
subscribers = ['gpcr','nw','ken','bur','tmsk','nsk','irk','mael','gpru','test']
#subscribers = ['reaction','gpcr','nw','ken','bur','mael','gpru','test']
#subscribers = ['nw','ken','bur','mael','test']
#subscribers = ['reaction','gpcr','gpru','nw','ken','bur']
#subscribers = ['reaction','gpcr','nw','ken']
#subscribers = ['nw','ken']
#subscribers = ['on_dem_cr','on_dem_all']
#subscribers = ['mael', 'test']
#subscribers = ['gpru']

#Маркер загрузки (для проверки отдавалась ли точка подписчику)
#Свободные: d e f g h j l p q v w x y z
marker = {
    'test': 't',
    'reaction': 'r',
    'gpcr': 'c',
    'on_dem_cr': '',
    'on_dem_all': '',
    'gpru': 'a',
    'nw': 'n',
    'ken': 'k',
    'bur': 'b',
    'mael':'m',
    'tmsk':'o',
    'nsk':'s',
    'irk':'i'}

#Критичность точек, отдаваемых на реагирование
critical = {
    'test': 1,
    'reaction': 120,
    'gpcr': 120,
    'on_dem_cr': 120,
    'on_dem_all': 1,
    'gpru': 0,
    'nw': 0,
    'ken': 0,
    'bur': 0,
    'mael':1,
    'tmsk':0,
    'nsk':0,
    'irk':0}

#Списки регионов
reg_list_cr = "('Ярославская область','Тверская область','Смоленская область','Рязанская область','Московская область','Москва','Калужская область','Ивановская область','Владимирская область','Брянская область')"
reg_list_nw = "('Архангельская область','Вологодская область','Мурманская область','Республика Карелия','Ленинградская область','Псковская область','Новгородская область','Калининградская область')"
reg_list_bur = "('Бурятская республика')"
reg_list_ken = "('Калининградская область')"
reg_list_ml = "('Республика Марий-Эл')"
reg_list_tmsk = "('Томская область')"
reg_list_nsk = "('Новосибирская область')"
reg_list_irk = "('Иркутская область')"

reg_list = {
    'test': reg_list_cr,
    'reaction': reg_list_cr,
    'gpcr': reg_list_cr,
    'on_dem_cr': reg_list_cr,
    'on_dem_all': reg_list_cr,
    'nw': reg_list_nw,
    'ken': reg_list_ken,
    'bur': reg_list_bur,
    'mael': reg_list_ml,
    'tmsk': reg_list_tmsk,
    'nsk': reg_list_nsk,
    'irk': reg_list_irk}

#e-mail для рассылки
mail_addr = {
    'test': ['chaus@firevolonter.ru','fly220@mail.ru'],
    'reaction': ['v.kastrel@firevolonter.ru'],
    'gpcr': ['i-semenov83@yandex.ru','dariiak13@gmail.com'],
    'gpru': ['sonya.kosacheva@greenpeace.org','egrudini@greenpeace.org','apulim@greenpeace.org','rebechenkom@gmail.com','ypetrenk@greenpeace.org','tekehiko.akiama@gmail.com'],
    'bur': ['kurmaevp@yandex.ru'], 'nw': ['tal-nataly@yandex.ru', 'gorchakovadarya21@gmail.com', 'hegny@list.ru', 'pikkuvesi@yandex.ru'],
    'ken': ['tal-nataly@yandex.ru', 'gorchakovadarya21@gmail.com', 'hegny@list.ru', 'pikkuvesi@yandex.ru'],
    'mael': ['nyamper@gmail.com', 'petr.dm@gmail.com'],
    'tmsk': ['sanforest@mail.ru','placeofspace@mail.ru'],
    'nsk':['sanforest@mail.ru','placeofspace@mail.ru'],
    'irk':['ano111.62@yandex.ru']}

#Период, за который производится выборка
period = '24 hours'
#period = '48 hours'
#period = '1 week'
#period = '1 months'

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
    log(logfile, "Creating folder %s..." %folder)
    base_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(base_path, root_path)
    result_path = os.path.join(result_path, folder)
    if not os.path.exists(result_path):
        try:
            os.mkdir(result_path)
            log(logfile, "Created %s" %result_path)
        except OSError:
            log(logfile, "Unable to create %s" %result_path)
    return result_path

#Создаем таблицу для выгрузки подписчику
def make_table_for(conn,cursor,src_tab,critical,period,reg_list,marker,whom):
    log(logfile, "Creating table for %s..." %whom)
    subs_tab = 'for_' + whom
    if (whom == 'reaction'):
        statements = (
        """
		DROP TABLE IF EXISTS %s
		"""%(subs_tab),
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
                geog GEOGRAPHY(POINT, 4326)
		)
		"""%(subs_tab),
        """
        INSERT INTO %(w)s (name,acq_date,acq_time,sat_sensor,region,rating,critical,peat_id,peat_district,peat_class,peat_fire,geog)
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
                %(s)s.geog
            FROM %(s)s
            WHERE date_time >= NOW() - INTERVAL '%(p)s' AND (critical >= %(c)s OR revision >= %(c)s) AND region in %(r)s AND POSITION('%(m)s' in marker) = 0
        """%{'w':subs_tab,'s':src_tab,'p':period,'c':critical[whom],'r':reg_list[whom],'m':marker[whom]},
        """
		UPDATE %s
			SET sat_sensor = 'VIIRS'
            WHERE sat_sensor = 'N'
        """%(subs_tab),
        """
		UPDATE %s
			SET sat_sensor = 'MODIS'
            WHERE sat_sensor <> 'VIIRS'
        """%(subs_tab),
        #"""
		#UPDATE %s
		#	SET name = ''
        #"""%(subs_tab),
        """
		UPDATE %s
			SET description =
            'Дата: ' || acq_date || '\n' ||
            'Время: ' || acq_time || '\n' ||
            'Сенсор: ' || sat_sensor || '\n' ||
            'Регион: ' || region || '\n' ||
            'Район: ' || peat_district || '\n' ||
            'Торфяник (ID): ' || peat_id || '\n' ||
            'Класс осушки: ' || peat_class || '\n' ||
            'Горимость торфяника: ' || peat_fire || '\n' ||
            'Предварительная оценка: ' || rating || '\n' ||
            'Критичность точки: ' || critical
        """%(subs_tab),
        """
		UPDATE %(s)s
			SET marker = marker || '%(m)s'
            WHERE date_time >= NOW() - INTERVAL '%(p)s' AND (critical >= %(c)s OR revision >= %(c)s) AND region in %(r)s AND POSITION('%(m)s' in marker) = 0
        """%{'s':src_tab,'p':period,'c':critical[whom],'r':reg_list[whom],'m':marker[whom]}
	   )
    elif (whom == 'gpru'):
        statements = (
        """
		DROP TABLE IF EXISTS %s
		"""%(subs_tab),
		"""
		CREATE TABLE %s (
                name VARCHAR(30),
                description VARCHAR(256),
                acq_date VARCHAR(10),
				acq_time VARCHAR(5),
				latitude NUMERIC,
				longitude NUMERIC,
                sat_sensor VARCHAR(5),
                region  VARCHAR(100),
                geog GEOGRAPHY(POINT, 4326)
		)
		"""%(subs_tab),
        """
        INSERT INTO %(w)s (acq_date,acq_time,latitude,longitude,sat_sensor,region,geog)
            SELECT
                %(s)s.acq_date,
                %(s)s.acq_time,
                %(s)s.latitude,
                %(s)s.longitude,
                %(s)s.satellite,
                %(s)s.region,
                %(s)s.geog
            FROM %(s)s
            WHERE date_time >= NOW() - INTERVAL '%(p)s' AND (critical >= %(c)s OR revision >= %(c)s) AND POSITION('%(m)s' in marker) = 0
            ORDER BY %(s)s.peat_id
        """%{'w':subs_tab,'s':src_tab,'p':period,'c':critical[whom],'m':marker[whom]},
        """
		UPDATE %s
			SET sat_sensor = 'VIIRS'
            WHERE sat_sensor = 'N'
        """%(subs_tab),
        """
		UPDATE %s
			SET sat_sensor = 'MODIS'
            WHERE sat_sensor <> 'VIIRS'
        """%(subs_tab),
        """
		UPDATE %s
			SET name = ''
        """%(subs_tab),
        """
		UPDATE %s
			SET description =
            'Дата: ' || acq_date || '\n' ||
            'Время: ' || acq_time || '\n' ||
            'Сенсор: ' || sat_sensor || '\n' ||
            'Регион: ' || region
        """%(subs_tab),
        """
		UPDATE %(s)s
			SET marker = marker || '%(m)s'
            WHERE date_time >= NOW() - INTERVAL '%(p)s' AND (critical >= %(c)s OR revision >= %(c)s) AND POSITION('%(m)s' in marker) = 0
        """%{'s':src_tab,'p':period,'c':critical[whom],'m':marker[whom]}
        )
    elif (whom in ['gpcr','bur','mael','tmsk','nsk','irk','nw','ken','on_dem_cr','on_dem_all','test']):
        statements = (
        """
		DROP TABLE IF EXISTS %s
		"""%(subs_tab),
		"""
		CREATE TABLE %s (
                name VARCHAR(30),
                description VARCHAR(500),
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
		"""%(subs_tab),
        """
        INSERT INTO %(w)s (name,acq_date,acq_time,latitude,longitude,sat_sensor,region,rating,critical,revision,peat_id,peat_district,peat_class,peat_fire,geog)
            SELECT
                %(s)s.name,
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
            WHERE date_time >= NOW() - INTERVAL '%(p)s' AND (critical >= %(c)s OR revision >= %(c)s) AND region in %(r)s AND POSITION('%(m)s' in marker) = 0
            ORDER BY %(s)s.peat_id
        """%{'w':subs_tab,'s':src_tab,'p':period,'c':critical[whom],'r':reg_list[whom],'m':marker[whom]},
        """
		UPDATE %s
			SET sat_sensor = 'VIIRS'
            WHERE sat_sensor = 'N'
        """%(subs_tab),
        """
		UPDATE %s
			SET sat_sensor = 'MODIS'
            WHERE sat_sensor <> 'VIIRS'
        """%(subs_tab),
        """
		UPDATE %s
			SET description =
            'Дата: ' || acq_date || '\n' ||
            'Время: ' || acq_time || '\n' ||
            'Сенсор: ' || sat_sensor || '\n' ||
            'Регион: ' || region || '\n' ||
            'Район: ' || peat_district || '\n' ||
            'Торфяник (ID): ' || peat_id || '\n' ||
            'Класс осушки: ' || peat_class || '\n' ||
            'Горимость торфяника: ' || peat_fire || '\n' ||
            'Предварительная оценка: ' || rating || '\n' ||
            'Критичность точки: ' || critical
        """%(subs_tab),
        """
		UPDATE %s
			SET description =
            'Дата: ' || acq_date || '\n' ||
            'Время: ' || acq_time || '\n' ||
            'Сенсор: ' || sat_sensor || '\n' ||
            'Регион: ' || region || '\n' ||
            'Район: ' || peat_district || '\n' ||
            'Торфяник (ID): ' || peat_id || '\n' ||
            'Класс осушки: ' || peat_class || '\n' ||
            'Горимость торфяника: ' || peat_fire || '\n' ||
            'Предварительная оценка: ' || rating || '\n' ||
            'Критичность точки: ' || critical || '\n' ||
            'Поправка: ' || revision
            WHERE revision IS NOT NULL
        """%(subs_tab),
        """
		UPDATE %s
			SET description =
            'Дата: ' || acq_date || '\n' ||
            'Время: ' || acq_time || '\n' ||
            'Сенсор: ' || sat_sensor || '\n' ||
            'Регион: ' || region
            WHERE peat_id IS NULL
        """%(subs_tab),
        """
		UPDATE %(s)s
			SET marker = marker || '%(m)s'
            WHERE date_time >= NOW() - INTERVAL '%(p)s' AND (critical >= %(c)s OR revision >= %(c)s) AND region in %(r)s AND POSITION('%(m)s' in marker) = 0
        """%{'s':src_tab,'p':period,'c':critical[whom],'r':reg_list[whom],'m':marker[whom]}
        )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log(logfile, 'The table created: %s'%(subs_tab))
    except IOError as e:
        log(logfile, 'Error creating subscribers tables: $s'%e)
    cursor.execute("SELECT count(*) FROM %s"%(subs_tab))
    return cursor.fetchone()[0]

def write_to_kml(subscriber,dst_file,dbserver,dbport,dbname,dbuser,dbpass):
    log(logfile, "Writting data to kml-file: %s..." %dst_file)
    src_tab = 'for_' + subscriber
    if os.path.isfile(dst_file):
        os.remove(dst_file)
        log(logfile, 'Owerwrite kml %s...'%(dst_file))
    else:
        log(logfile, 'Create new kml %s...'%(dst_file))
    command = """ogr2ogr -f "KML" %(d)s PG:"host=%(h)s user=%(u)s dbname=%(b)s password=%(w)s port=%(p)s" %(s)s"""%{'d':dst_file,'s':src_tab,'h':dbserver,'u':dbuser,'b':dbname,'w':dbpass,'p':dbport}
    os.system(command)
    log(logfile, 'Done.')

#Запись файла "file" из каталога "from_dir" на я-диск в каталог "to_dir" подкаталог "subscriber"
def write_to_yadisk(file, from_dir, to_dir, subscriber, yadisk_token):
    log(logfile, "Writing file %s to Yandex disk..." %file)
    y = yadisk.YaDisk(token=yadisk_token)
    to_dir = to_dir + subscriber
    p = from_dir.split(from_dir)[1].strip(os.path.sep)
    dir_path = posixpath.join(to_dir, p)
    file_path = posixpath.join(dir_path, file)
    p_sys = p.replace("/", os.path.sep)
    in_path = os.path.join(from_dir, p_sys, file)
    try:
        y.upload(in_path, file_path, overwrite = True)
        log(logfile, 'File written to yadisk %s.'%(file_path))
    except yadisk.exceptions.PathExistsError:
        log(logfile, 'Path not exist %s.'%(dir_path))
        pass

#Send an email with an attachment
def send_email_with_attachment(inifile, date, emails, path_to_attach, file_to_attach, num_points):
    log(logfile, "Sending e-mail to addresses: %s..." %emails)
    # extract server and from_addr from config
    [host,from_addr,user,pwd] = get_config(inifile, "smtp", ["server", "from_addr", "user", "pwd"])

    header = 'Content-Disposition', 'attachment; filename="%s"' % file_to_attach
    if period == '24 hours':
        if num_points > 0:
            subject = "Daily points per %(d)s (%(n)s points)"%{'d':date, 'n':num_points}
        else:
            subject = "Daily points per %s (no any points)"%(date)
        body_text = "In the attachment firepoints for last day.\r\nEmail to dist_mon@firevolonter.ru if you find any errors or inaccuracies."
    else:
        if num_points > 0:
            subject = "Points per last %(p)s (%(n)s points)"%{'p':period, 'n':num_points}
        else:
            subject = "Points per last %s (no any points)"%(period)
        body_text = "In the attachment firepoints for last day.\r\nEmail to dist_mon@firevolonter.ru if you find any errors or inaccuracies."

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
    log(logfile, 'Mail sended.')

#Удаляем временные таблицы
def drop_whom_table(conn,cursor, whom):
    subs_tab = 'for_' + whom
    log(logfile, "Dropping table %s" %subs_tab)
    try:
        cursor.execute("DROP TABLE IF EXISTS %s"%(subs_tab))
        conn.commit()
        log(logfile, "Table dropped.")
    except IOError as e:
        log(logfile, 'Error dropping table:$s' %e)

def send_to_subscribers_job():
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d',currtime)
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)

    logfile = get_log_file(date)
    log(logfile, '--------------------------------------------------------------------------------')
    log(logfile, 'Process [send_to_subscribers.py] started at %s'%(cdate))

    # extract params from config
    [dbserver,dbport,dbname,dbuser,dbpass] = get_config(inifile, "db", ["dbserver","dbport","dbname", "dbuser", "dbpass"])
    [year_tab] = get_config(inifile, "tables", ["year_tab"])
    [data_root,result_folder] = get_config(inifile, "path", ["data_root", "result_folder"])
    [to_dir,yadisk_token] = get_config(inifile, "yadisk", ["yadisk_out_path", "yadisk_token"])

    #connecting to database
    conn = psycopg2.connect(dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()

    #Создаем каталоги
    result_dir = get_path(data_root,result_folder)

    for subscriber in subscribers:
        num_points = make_table_for(conn,cursor,year_tab,critical,period,reg_list,marker,subscriber)
        if period == '24 hours':
            dst_file_name = '%(d)s_%(s)s.kml'%{'d': date, 's': subscriber}
        else:
            period_mod = period
            period_mod = period_mod.replace(' ','_')
            dst_file_name = '%(d)s_%(s)s_%(p)s.kml'%{'d': date, 's': subscriber, 'p':period_mod}
        dst_file = os.path.join(result_dir,dst_file_name)
        write_to_kml(subscriber,dst_file,dbserver,dbport,dbname,dbuser,dbpass)
        write_to_yadisk(dst_file_name, result_dir, to_dir, subscriber, yadisk_token)
        send_email_with_attachment(inifile, date, mail_addr[subscriber], result_dir, dst_file_name, num_points)
        drop_whom_table(conn,cursor,subscriber)

    cursor.close
    conn.close

    currtime = time.localtime()
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
    log(logfile, 'Process [send_to_subscribers.py] stopped at %s'%(cdate))

#main
if __name__ == "__main__":
    send_to_subscribers_job()
