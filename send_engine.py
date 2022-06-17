#-------------------------------------------------------------------------------
# Name:        send_engine
# Purpose:
# Author:      Chaus
# Created:     13.05.2020
#-------------------------------------------------------------------------------

import os, time
from falogging import log, start_logging, stop_logging
from faservice import get_config, get_tuple_cursor, close_conn, get_path, smf_new_topic, str_to_lst
from faservice import write_to_kml, write_to_yadisk, send_email_with_attachment, send_email_message, send_doc_to_telegram, send_to_telegram
from requester import make_tlg_stat_msg, make_zone_stat_msg, make_oopt_stat_msg, make_oopt_buf_stat_msg, make_smf_stat_msg, check_vip_zones, get_oopt_for_region, get_oopt_for_ids
#Создаем таблицу для выгрузки подписчикам
def make_subs_table(conn,cursor,src_tab,crit_or_peat,limit,period,reg_list,whom,is_incremental,filter_tech):
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
        WHERE date_time >= TIMESTAMP 'today' - INTERVAL '%(p)s' AND "date_time" < TIMESTAMP 'today' AND %(c)s >= %(l)s AND region in %(r)s AND NOT((tech IS NOT NULL) AND %(t)s)
        ORDER BY %(s)s.peat_id
    """%{'w':subs_tab,'s':src_tab,'p':period,'c':crit_or_peat,'l':limit,'r':reg_list,'m':marker, 't':filter_tech},
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
        WHERE date_time >= TIMESTAMP 'today' - INTERVAL '%(p)s' AND "date_time" < TIMESTAMP 'today' AND NOT((tech IS NOT NULL) AND %(t)s)
    """%{'w':subs_tab,'s':src_tab,'p':period,'m':marker, 't':filter_tech},
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
        WHERE "date_time" > TIMESTAMP 'today' AND %(c)s >= %(l)s AND region in %(r)s AND (whom is Null OR POSITION('%(m)s' in whom) = 0) AND NOT((tech IS NOT NULL) AND %(t)s)
        ORDER BY %(s)s.peat_id
    """%{'w':subs_tab,'s':src_tab,'p':period,'c':crit_or_peat,'l':limit,'r':reg_list,'m':marker, 't':filter_tech},
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
        WHERE "date_time" > TIMESTAMP 'today' AND (whom is Null OR POSITION('%(m)s' in whom) = 0) AND NOT((tech IS NOT NULL) AND %(t)s)
    """%{'w':subs_tab,'s':src_tab,'p':period,'m':marker, 't':filter_tech},
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
def fill_send_times(conn,cursor,subs_tab,subs_id,zero_time, period):
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
        cursor.execute("UPDATE %(s)s SET send_times = '%(h)s' WHERE subs_id = %(i)s"%{'s':subs_tab,'h':send_hours,'i':subs_id})
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
        drop_temp_file(dst_file)
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

def drop_temp_file(the_file):
    try:
        if os.path.isfile(the_file):
            os.remove(the_file)
    except Exception as e:
        log('Cannot remove files:$s' %e)


def make_mail_attr(date, period, num_points):
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
    return subject, body_text

def set_name(conn, cursor, subs_tab, subs_id):
    log('Setting name for ID %s'%subs_id)
    new_name = 's-' + str(subs_id)
    cursor.execute("UPDATE %(s)s SET subs_name = '%(n)s' WHERE subs_id = %(i)s"%{'s':subs_tab,'n':new_name,'i':subs_id})
    conn.commit()
    log('Setting name done.')

def send_to_subscribers_job():

    start_logging('send_engine.py')

    currtime = time.localtime()
    date = time.strftime('%Y-%m-%d',currtime)
    now_hour = time.strftime('%H',currtime)

    # extract params from config
    [year_tab, subs_tab] = get_config("tables", ["year_tab","subs_tab"])
    [data_root,temp_folder] = get_config("path", ["data_root", "temp_folder"])
    [to_dir] = get_config("yadisk", ["yadisk_out_path"])
    [url] = get_config('telegramm', ['url'])
    [outline] = get_config('tables', ['vip_zones'])

    #connecting to database
    conn, cursor = get_tuple_cursor()

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
	#send_first_time - varchar(5) - время рассылки
	#send_period - integer - периодичность рассылки
	#send_times - varchar() - список временных меток для рассылки
    #vip_zones - boolean - рассылать ли информацию по зонам особого внимания
    #send_empty - отправлять или нет пустой файл
    #ya_disk - писать или нет файл на яндекс-диск

    cursor.execute("SELECT * FROM %s WHERE active"%(subs_tab))
    subscribers = cursor.fetchall()

    #Создаем каталог для записи временных файлов
    result_dir = get_path(data_root,temp_folder)

    for subs in subscribers:
        if subs.subs_name == None:
            set_name(conn, cursor, subs_tab, subs.subs_id)
        log('Processing for %s...'%(subs.subs_name))
        if subs.send_times == None:
            sendtimelist = fill_send_times(conn,cursor,subs_tab,subs.subs_id,subs.send_first_time, subs.send_period).split(',')
        else:
            sendtimelist = subs.send_times.split(',')

        if now_hour in sendtimelist and (subs.email_point or subs.teleg_point):
            log('Sending points now!')
            iteration = sendtimelist.index(now_hour)
            is_increment = (iteration != 0)
            if subs.crit_or_fire == 'crit':
                log('Making critical-limited table...')
                num_points = make_subs_table(conn,cursor,year_tab,'critical',subs.critical,subs.point_period,subs.regions,subs.subs_id,is_increment,subs.filter_tech)
            elif subs.crit_or_fire == 'fire':
                log('Making fire-limited table...')
                num_points = make_subs_table(conn,cursor,year_tab,'peat_fire',subs.peatfire,subs.point_period,subs.regions,subs.subs_id,is_increment,subs.filter_tech)
            else:
                log('Making zero-critical table...')
                num_points = make_subs_table(conn,cursor,year_tab,'critical',0,subs.point_period,subs.regions,subs.subs_id,is_increment,subs.filter_tech)
            if num_points > 0 or subs.send_empty:
                dst_file_name = make_file_name(subs.point_period, date, subs.subs_name, result_dir,iteration)
                dst_file = os.path.join(result_dir,dst_file_name)

                log('Creating maillist...')
                maillist = subs.email.replace(' ','').split(',')
                log('Creating kml file...')
                write_to_kml(dst_file,subs.subs_id)
                if subs.email_point:
                    subject, body_text = make_mail_attr(date, subs.point_period, num_points)
                    try:
                        send_email_with_attachment(maillist, subject, body_text, [dst_file])
                    except IOError as e:
                        log('Error seneding e-mail. Error:$s'%e)
                if subs.teleg_point:
                    doc = open(dst_file, 'rb')
                    send_doc_to_telegram(url, subs.telegramm, doc)
                    send_to_telegram(url, subs.telegramm, 'В файле %s точек.' %num_points)
                log('Dropping temp files...')
                drop_temp_file(dst_file)
            else:
                log('Don`t send zero-point file.')
            if now_hour == sendtimelist[0] and subs.ya_disk:
                log('Writing to yadisk...')
                subs_folder = 'for_s%s' %str(subs.subs_name)
                write_to_yadisk(dst_file_name, result_dir, to_dir, subs_folder)
            log('Dropping tables...')
            drop_whom_table(conn,cursor,subs.subs_id)
        else:
            log('Do anything? It`s not time yet!')

#        if now_hour == sendtimelist[0] and subs.vip_zones:
#            points_count, zones = check_vip_zones(outline, subs.stat_period)
#            if points_count > 0:
#                msg = 'Новых точек\r\nв зонах особого внимания: %s\r\n\r\n' %points_count
#                for (zone, num_points) in zones:
#                    msg = msg + '%s - %s\r\n' %(zone, num_points)
#                send_to_telegram(url, subs.telegramm, msg)
        period = '%sh'%subs.stat_period
        if subs.vip_zones:
            log('Checking zones stat for %s...'%str(subs.subs_name))
            zone_list = str_to_lst(subs.zones[2:-2])
            msg = make_zone_stat_msg(zone_list, period)
            if msg != '':
                log('Sending zones stat to telegram...')
                send_to_telegram(url, subs.telegramm, msg)

        if subs.check_oopt:
            log('Checking oopt stat for %s...'%str(subs.subs_name))
            oopt_list = ''
            if subs.oopt_zones != None:
                #oopt_list = str_to_lst(subs.oopt_zones[2:-2])
                oopt_list = get_oopt_for_ids(subs.oopt_zones)
            elif subs.oopt_regions != None:
                oopt_list = get_oopt_for_region(subs.oopt_regions)
            log('List of zones for checking: %s'%oopt_list)
            if oopt_list != '':
                msg = make_oopt_stat_msg(oopt_list, period)
                if msg == '':
                    log('No points in OOPT. Stat is not sending to %s.'%str(subs.subs_name))
                else:
                    log('Sending oopt stat to telegram...')
                    send_to_telegram(url, subs.telegramm, msg)
                if subs.check_oopt_buf:
                    msg2 = make_oopt_buf_stat_msg(oopt_list, period)
                    if msg2 == '':
                        log('No points in OOPT buffers. Stat is not sending to %s.'%str(subs.subs_name))
                    else:
                        log('Sending oopt buffers stat to telegram...')
                        send_to_telegram(url, subs.telegramm, msg2)
            else:
                log('Error sending oopt stat to telegram. Oopt list is Null!!!')

        if now_hour == sendtimelist[0] and (subs.teleg_stat or subs.email_stat):
            reg_list = str_to_lst(subs.regions[2:-2])
            msg = make_tlg_stat_msg(reg_list, period, subs.critical)
            if subs.teleg_stat:
                log('Sending stat to telegram...')
                send_to_telegram(url, subs.telegramm, msg)
            if subs.email_stat:
                log('Sending stat to email...')
                subject = "Statistic per last %(p)s"%{'p':subs.stat_period}
                maillist = subs.email.replace(' ','').split(',')
                send_email_message(maillist, subject, msg)

        if now_hour == '24':
            [smf_url, smf_user, smf_pass] = get_config("smf", ["smf_url", "smf_user", "smf_pass"])
            [period, critical_limit] = get_config("statistic", ["period", "critical_limit"])
            [reg_list_cr] = get_config("reglists", ["cr"])
            fdate=time.strftime('%d-%m-%Y',currtime)
            smf_msg = make_smf_stat_msg(reg_list_cr, period, critical_limit)
            smf_new_topic(smf_url, smf_user, smf_pass, 13.0, fdate, smf_msg)

    close_conn(conn, cursor)
    stop_logging('send_engine.py')

#main
if __name__ == "__main__":
    send_to_subscribers_job()