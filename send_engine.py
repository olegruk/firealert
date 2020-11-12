#-------------------------------------------------------------------------------
# Name:        send_engine
# Purpose:
# Author:      Chaus
# Created:     13.05.2020
#-------------------------------------------------------------------------------

import os, time, sys
import yadisk
import posixpath
import smtplib
from email import encoders
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate
from falogging import log, start_logging, stop_logging
from faservice import get_config, get_cursor, close_conn, get_path, write_to_kml

#Создаем таблицу для выгрузки подписчикам
def make_subs_table(conn,cursor,src_tab,crit_or_peat,limit,period,reg_list,whom,is_incremental):
    log("Creating table for subs_id:%s..." %whom)
    subs_tab = 'for_s%s' %str(whom)
    marker = '[s%s]' %str(whom)
    period = '%s hours' %period

    statements_regional_yesterday = (
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
			critical SMALLINT,
            geog GEOGRAPHY(POINT, 4326)
	)
	"""%(subs_tab),
    """
    INSERT INTO %(w)s (name,acq_date,acq_time,latitude,longitude,sat_sensor,region,critical,peat_id,peat_district,peat_class,peat_fire,geog)
        SELECT
            %(s)s.name,
            %(s)s.acq_date,
            %(s)s.acq_time,
            %(s)s.latitude,
            %(s)s.longitude,
            %(s)s.satellite,
            %(s)s.region,
            %(s)s.critical,
            %(s)s.peat_id,
            %(s)s.peat_district,
            %(s)s.peat_class,
            %(s)s.peat_fire,
            %(s)s.geog
        FROM %(s)s
        WHERE date_time >= TIMESTAMP 'today' - INTERVAL '%(p)s' AND "date_time" < TIMESTAMP 'today' AND %(c)s >= %(l)s AND region in %(r)s
        ORDER BY %(s)s.peat_id
    """%{'w':subs_tab,'s':src_tab,'p':period,'c':crit_or_peat,'l':limit,'r':reg_list,'m':marker},
    """
	UPDATE %(s)s
		SET whom = whom || '%(m)s'
        WHERE date_time >= TIMESTAMP 'today' - INTERVAL '%(p)s' AND "date_time" < TIMESTAMP 'today' AND %(c)s >= %(l)s AND region in %(r)s AND POSITION('%(m)s' in whom) = 0
    """%{'s':src_tab,'p':period,'c':crit_or_peat,'l':limit,'r':reg_list,'m':marker},
    """
	UPDATE %(s)s
		SET whom = '%(m)s'
        WHERE date_time >= TIMESTAMP 'today' - INTERVAL '%(p)s' AND "date_time" < TIMESTAMP 'today' AND %(c)s >= %(l)s AND region in %(r)s AND whom is Null
    """%{'s':src_tab,'p':period,'c':crit_or_peat,'l':limit,'r':reg_list,'m':marker},
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
        'Критичность точки: ' || critical
    """%(subs_tab),
    """
	UPDATE %s
		SET description =
        'Дата: ' || acq_date || '\n' ||
        'Время: ' || acq_time || '\n' ||
        'Сенсор: ' || sat_sensor || '\n' ||
        'Регион: ' || region
        WHERE peat_id IS NULL
    """%(subs_tab)
    )

    statements_allrussia_yesterday = (
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
        WHERE date_time >= TIMESTAMP 'today' - INTERVAL '%(p)s' AND "date_time" < TIMESTAMP 'today'
    """%{'w':subs_tab,'s':src_tab,'p':period,'m':marker},
    """
	UPDATE %(s)s
		SET whom = whom || '%(m)s'
        WHERE date_time >= TIMESTAMP 'today' - INTERVAL '%(p)s' AND "date_time" < TIMESTAMP 'today' AND POSITION('%(m)s' in whom) = 0
    """%{'s':src_tab,'p':period,'c':crit_or_peat,'l':limit,'r':reg_list,'m':marker},
    """
	UPDATE %(s)s
		SET whom = '%(m)s'
        WHERE date_time >= TIMESTAMP 'today' - INTERVAL '%(p)s' AND "date_time" < TIMESTAMP 'today' AND whom is Null
    """%{'s':src_tab,'p':period,'c':crit_or_peat,'l':limit,'r':reg_list,'m':marker},
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
    """%(subs_tab)
    )

    statements_regional_incremental = (
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
			critical SMALLINT,
            geog GEOGRAPHY(POINT, 4326)
	)
	"""%(subs_tab),
    """
    INSERT INTO %(w)s (name,acq_date,acq_time,latitude,longitude,sat_sensor,region,critical,peat_id,peat_district,peat_class,peat_fire,geog)
        SELECT
            %(s)s.name,
            %(s)s.acq_date,
            %(s)s.acq_time,
            %(s)s.latitude,
            %(s)s.longitude,
            %(s)s.satellite,
            %(s)s.region,
            %(s)s.critical,
            %(s)s.peat_id,
            %(s)s.peat_district,
            %(s)s.peat_class,
            %(s)s.peat_fire,
            %(s)s.geog
        FROM %(s)s
        WHERE "date_time" > TIMESTAMP 'today' AND %(c)s >= %(l)s AND region in %(r)s AND (whom is Null OR POSITION('%(m)s' in whom) = 0)
        ORDER BY %(s)s.peat_id
    """%{'w':subs_tab,'s':src_tab,'p':period,'c':crit_or_peat,'l':limit,'r':reg_list,'m':marker},
    """
	UPDATE %(s)s
		SET whom = whom || '%(m)s'
        WHERE "date_time" > TIMESTAMP 'today' AND %(c)s >= %(l)s AND region in %(r)s AND POSITION('%(m)s' in whom) = 0
    """%{'s':src_tab,'p':period,'c':crit_or_peat,'l':limit,'r':reg_list,'m':marker},
    """
	UPDATE %(s)s
		SET whom = '%(m)s'
        WHERE "date_time" > TIMESTAMP 'today' AND %(c)s >= %(l)s AND region in %(r)s AND whom is Null
    """%{'s':src_tab,'p':period,'c':crit_or_peat,'l':limit,'r':reg_list,'m':marker},
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
        'Критичность точки: ' || critical
    """%(subs_tab),
    """
	UPDATE %s
		SET description =
        'Дата: ' || acq_date || '\n' ||
        'Время: ' || acq_time || '\n' ||
        'Сенсор: ' || sat_sensor || '\n' ||
        'Регион: ' || region
        WHERE peat_id IS NULL
    """%(subs_tab)
    )

    statements_allrussia_incremental = (
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
        WHERE "date_time" > TIMESTAMP 'today' AND (whom is Null OR POSITION('%(m)s' in whom) = 0)
    """%{'w':subs_tab,'s':src_tab,'p':period,'m':marker},
    """
	UPDATE %(s)s
		SET whom = whom || '%(m)s'
        WHERE "date_time" > TIMESTAMP 'today' AND POSITION('%(m)s' in whom) = 0
    """%{'s':src_tab,'p':period,'c':crit_or_peat,'l':limit,'r':reg_list,'m':marker},
    """
	UPDATE %(s)s
		SET whom = '%(m)s'
        WHERE "date_time" > TIMESTAMP 'today' AND whom is Null
    """%{'s':src_tab,'p':period,'c':crit_or_peat,'l':limit,'r':reg_list,'m':marker},
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
    """%(subs_tab)
    )

    if reg_list == "('Россия')":
        if is_incremental:
            statements = statements_allrussia_incremental
        else:
            statements = statements_allrussia_yesterday
    else:
        if is_incremental:
            statements = statements_regional_incremental
        else:
            statements = statements_regional_yesterday

    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log('The table created: subs_id:%s'%(whom))
    except IOError as e:
        log('Error creating subscribers tables: $s'%e)
    cursor.execute("SELECT count(*) FROM %s"%(subs_tab))
    return cursor.fetchone()[0]

#Запись файла "file" из каталога "from_dir" на я-диск в каталог "to_dir" в подкаталог для "whom"
def write_to_yadisk(file, from_dir, to_dir, yadisk_token, whom):
    subs_folder = 'for_s%s' %str(whom)
    log("Writing file %s to Yandex disk..." %file)
    y = yadisk.YaDisk(token=yadisk_token)
    to_dir = to_dir + subs_folder
    p = from_dir.split(from_dir)[1].strip(os.path.sep)
    dir_path = posixpath.join(to_dir, p)
    if not y.exists(dir_path):
        try:
            y.mkdir(dir_path)
            log('Path created on yadisk %s.'%(dir_path))
        except yadisk.exceptions.PathExistsError:
            log('Path cannot be created %s.'%(dir_path))
    file_path = posixpath.join(dir_path, file)
    p_sys = p.replace("/", os.path.sep)
    in_path = os.path.join(from_dir, p_sys, file)
    try:
        y.upload(in_path, file_path, overwrite = True)
        log('File written to yadisk %s.'%(file_path))
    except yadisk.exceptions.PathExistsError:
        log('Path not exist %s.'%(dir_path))
        pass

#Send an email with an attachment
def send_email_with_attachment(date, emails, path_to_attach, file_to_attach, num_points, period):
    log("Sending e-mail to addresses: %s..." %emails)
    # extract server and from_addr from config
    [host,from_addr,user,pwd] = get_config("smtp", ["server", "from_addr", "user", "pwd"])

    header = 'Content-Disposition', 'attachment; filename="%s"' % file_to_attach
    if period == 24:
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
        log(msg)
        sys.exit(1)

    mailserver = smtplib.SMTP(host,587)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.ehlo()
    mailserver.login(user, pwd)
    mailserver.sendmail(from_addr, emails, msg.as_string())
    mailserver.quit()
    log('Mail sended.')

#Удаляем временные таблицы
def drop_whom_table(conn,cursor, whom):
    subs_tab = 'for_s%s' %str(whom)
    log("Dropping table %s" %subs_tab)
    try:
        cursor.execute("DROP TABLE IF EXISTS %s"%(subs_tab))
        conn.commit()
        log("Table dropped.")
    except IOError as e:
        log('Error dropping table:$s' %e)

# Заполняем в таблице подписчиков поле с временами рассылки, если не было заполнено
def fill_send_times(conn,cursor,subs_tab,time_field,subs_id,zero_time, period):
    log('Creating send times for %s.'%(subs_id))
    if (zero_time != None) and (period != None):
        zero_hour = int(zero_time.split(":")[0])
        send_hours = ''
        if 24 % period == 0:
            num_of_times = int(24//period)
        else:
            num_of_times = int(24//period) + 1
        for i in range(num_of_times):
            new_hour = (zero_hour + i*period) % 24
            if new_hour < 10:
                send_hours = send_hours + '0' + str(new_hour)
            else:
                send_hours = send_hours + str(new_hour)
            if i < num_of_times - 1:
                send_hours = send_hours + ","
        cursor.execute("UPDATE %(s)s SET %(t)s = '%(h)s' WHERE subs_id = %(i)s"%{'s':subs_tab,'t':time_field,'h':send_hours,'i':subs_id})
        conn.commit()
        return send_hours
    else:
        return ''

def make_file_name(period, date, whom, result_dir,iter):
    if iter == 0:
        suff = ''
    else:
        suff = '_inc%s' %str(iter)
    if period == 24:
        dst_file_name = '%(d)s_%(s)s%(i)s.kml'%{'d': date, 's': whom, 'i':suff}
    else:
        period_mod = period
        period_mod = period_mod.replace(' ','_')
        dst_file_name = '%(d)s_%(s)s_%(p)s%(i)s.kml'%{'d': date, 's': whom, 'p':period_mod, 'i':suff}
    dst_file = os.path.join(result_dir,dst_file_name)
    if os.path.isfile(dst_file):
        iter = iter + 1
        dst_file_name = make_file_name(period, date, whom, result_dir,iter)
    return dst_file_name

def drop_temp_files(result_dir):
    for the_file in os.listdir(result_dir):
        file_path = os.path.join(result_dir, the_file)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            log('Cannot remove files:$s' %e)

def send_to_subscribers_job():

    start_logging('send_engine.py')

    currtime = time.localtime()
    date = time.strftime('%Y-%m-%d',currtime)
    now_hour = time.strftime('%H',currtime)

    # extract params from config
    [year_tab, subs_tab] = get_config("tables", ["year_tab","subs_tab"])
    [data_root,temp_folder] = get_config("path", ["data_root", "temp_folder"])
    [to_dir,yadisk_token] = get_config("yadisk", ["yadisk_out_path", "yadisk_token"])

    #connecting to database
    conn, cursor = get_cursor()

    #Загружаем данные о подписчиках
    #subs_id - serial - автоидентификатор
    #subs_name - varchar(10) - имя подписчика, для удобства ориентирования в подписках
    #active - boolean - признак активной подписки
    #regions - varchar() - список регионов
    #email - varchar() - список адресов подписчика
    #telegramm - varchar(20) - список телеграмм-чатов подписчика
	#email_stat - boolean - слать статистику по почте?
	#teleg_stat - boolean - слать статистику в телеграмм?
	#email_point - boolean - слать точки по почте?
	#teleg_point - boolean - слать точки в телеграмм?
	#stat_period - integer - период в часах, за который выдается статистика
	#point_period - integer - период в часах, за который выбираются точки
	#crit_or_fire - varchar(4) - критерий отбора точек, критичность точки - 'crit' или горимость торфа - 'fire'
	#critical - integer - порог критичности для отбора точек
	#peatfire - integer - порог горимости торфяника для отбора точек
	#email_first_time - varchar(5) - время рассылки по почте
	#email_period - integer - периодичность рассылки по почте
	#email_times - varchar() - список временных меток для рассылки по почте
	#teleg_first_time - varchar(5) - время рассылки по телеграмм
	#teleg_period - integer - периодичность рассылки по телеграмм
	#teleg_times - varchar() - список временных меток для рассылки по телеграмм
    #vip_zones - boolean - рассылать ли информацию по зонам особого внимания

    cursor.execute("SELECT * FROM %s WHERE active"%(subs_tab))
    subscribers = cursor.fetchall()

    #Создаем каталог для записи временных файлов
    result_dir = get_path(data_root,temp_folder)

    for subs in subscribers:
        if subs.subs_name == NULL:
            subs.subs_name = subs.subs_id
        log('Processing for %s...'%(subs.subs_name))
        if subs.email_times == None:
            emailtimelist = fill_send_times(conn,cursor,subs_tab,'email_times',subs.subs_id,subs.email_first_time, subs.email_period).split(',')
        else:
            emailtimelist = subs.email_times.split(',')
        if subs.teleg_times == None:
            telegtimelist = fill_send_times(conn,cursor,subs_tab,'teleg_times',subs.subs_id,subs.teleg_first_time, subs.teleg_period).split(',')
        else:
            telegtimelist = subs.teleg_times.split(',')

        if now_hour in emailtimelist and subs.email_point:
            log('Sending mail now!')
            is_increment = not(now_hour == emailtimelist[0])
            if subs.crit_or_fire == 'crit':
                log('Making critical-limited table...')
                num_points = make_subs_table(conn,cursor,year_tab,'critical',subs.critical,subs.point_period,subs.regions,subs.subs_id,is_increment)
            elif subs.crit_or_fire == 'fire':
                log('Making fire-limited table...')
                num_points = make_subs_table(conn,cursor,year_tab,'peat_fire',subs.peatfire,subs.point_period,subs.regions,subs.subs_id,is_increment)
            else:
                log('Making zero-critical table...')
                num_points = make_subs_table(conn,cursor,year_tab,'critical',0,subs.point_period,subs.regions,subs.subs_id,is_increment)

            dst_file_name = make_file_name(subs.point_period, date, subs.subs_name, result_dir,0)
            dst_file = os.path.join(result_dir,dst_file_name)

            log('Creating maillist...')
            maillist = subs.email.replace(' ','').split(',')
            log('Creating kml file...')
            write_to_kml(dst_file,subs.subs_id)
            log('Sending e-mail...')
            send_email_with_attachment(date, maillist, result_dir, dst_file_name, num_points, subs.point_period)
            if now_hour == emailtimelist[0]:
                log('Writing to yadisk...')
                write_to_yadisk(dst_file_name, result_dir, to_dir, yadisk_token, subs.subs_name)
            if now_hour == emailtimelist[-1]:
                log('Dropping temp files...')
                drop_temp_files(result_dir)
            log('Dropping tables...')
            drop_whom_table(conn,cursor,subs.subs_id)
        else:
            log('Sending mail? It`s not time yet!')

    close_conn(conn, cursor)
    stop_logging('send_engine.py')

#main
if __name__ == "__main__":
    send_to_subscribers_job()