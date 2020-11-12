#-------------------------------------------------------------------------------
# Name:        send_engine
# Purpose:
# Author:      Chaus
# Created:     13.05.2020
#-------------------------------------------------------------------------------

import os, time
from falogging import log
from faservice import get_config, get_db_config, get_cursor, close_conn

#Создаем таблицу для выгрузки подписчикам
def make_reqst_table(conn,cursor,src_tab,crit_or_peat,limit, from_time, period, reg_list, whom,is_incremental):
    log("Creating table for subs_id:%s..." %whom)
    subs_tab = 'for_%s' %str(whom)
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
        WHERE date_time > TIMESTAMP '%(t)s' - INTERVAL '%(p)s' AND "date_time" <= TIMESTAMP '%(t)s' AND %(c)s >= %(l)s AND region in %(r)s
        ORDER BY %(s)s.peat_id
    """%{'w':subs_tab,'s':src_tab, 't':from_time,'p':period,'c':crit_or_peat,'l':limit,'r':reg_list},
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
        WHERE date_time > TIMESTAMP '%(t)s' - INTERVAL '%(p)s' AND "date_time" <= TIMESTAMP '%(t)s'
    """%{'w':subs_tab,'s':src_tab,'t':from_time,'p':period},
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
        statements = statements_allrussia_yesterday
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

#Создаем таблицу для выгрузки подписчикам
def make_reqst_for_circle(conn,cursor,src_tab,crit_or_peat,limit, from_time, period, circle, whom):
    log("Creating table of points in circle for subs_id:%s..." %whom)
    subs_tab = 'for_%s' %str(whom)
#    period = '%s hours' %period
    cent_x = circle[0]
    cent_y = circle[1]
    radius = circle[2]
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
        WHERE date_time > TIMESTAMP '%(t)s' - INTERVAL '%(p)s' AND "date_time" <= TIMESTAMP '%(t)s' AND %(c)s >= %(l)s AND 
        ST_DWithin(%(s)s.geog, ST_GeogFromText('SRID=4326;POINT(%(x)s %(y)s)'), %(r)s)
    """%{'w':subs_tab,'s':src_tab, 't':from_time,'p':period,'c':crit_or_peat,'l':limit,'x':cent_x,'y':cent_y,'r':radius},
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

    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log('The table created: subs_id:%s'%(whom))
    except IOError as e:
        log('Error creating subscribers tables: $s'%e)
    cursor.execute("SELECT count(*) FROM %s"%(subs_tab))
    return cursor.fetchone()[0]


# Сохраняем созданную таблицу в kml-файл для последующей отправки подписчикам
def write_to_kml(dbserver,dbport,dbname,dbuser,dbpass,dst_file,whom):
    subs_tab = 'for_%s' %str(whom)
    log("Writting data from %(s)s table to kml-file: %(f)s..." %{'s':subs_tab, 'f':dst_file})
    if os.path.isfile(dst_file):
        os.remove(dst_file)
        log('Owerwrite kml %s...'%(dst_file))
    else:
        log('Create new kml %s...'%(dst_file))
    command = """ogr2ogr -f "KML" %(d)s PG:"host=%(h)s user=%(u)s dbname=%(b)s password=%(w)s port=%(p)s" %(s)s"""%{'d':dst_file,'s':subs_tab,'h':dbserver,'u':dbuser,'b':dbname,'w':dbpass,'p':dbport}
    os.system(command)
    log('Done.')

#Удаляем временные таблицы
def drop_whom_table(conn,cursor, whom):
    subs_tab = 'for_%s' %str(whom)
    log("Dropping table %s" %subs_tab)
    try:
        cursor.execute("DROP TABLE IF EXISTS %s"%(subs_tab))
        conn.commit()
        log("Table dropped.")
    except IOError as e:
        log('Error dropping table:$s' %e)

def make_file_name(period, date, whom, result_dir,iter):
    if iter == 0:
        suff = ''
    else:
        suff = '_inc%s' %str(iter)
    if period == '24 hours':
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

def request_data(whom, lim_for, limit, from_time, period, regions, result_dir):

    currtime = time.localtime()
    date = time.strftime('%Y-%m-%d',currtime)

    # extract params from config
    [dbserver,dbport,dbname,dbuser,dbpass] = get_db_config("db", ["dbserver","dbport","dbname", "dbuser", "dbpass"])
    [year_tab] = get_config("tables", ["year_tab"])

    #connecting to database
    conn, cursor = get_cursor()

    num_points = make_reqst_table(conn,cursor,year_tab,lim_for,limit,from_time,period,regions,whom, False)
    dst_file_name = make_file_name(period, date, whom, result_dir,0)
    dst_file = os.path.join(result_dir,dst_file_name)
    write_to_kml(dbserver,dbport,dbname,dbuser,dbpass,dst_file,whom)
    drop_whom_table(conn,cursor,whom)

    close_conn(conn, cursor)

    return dst_file, num_points

def request_for_circle(whom, lim_for, limit, from_time, period, circle, result_dir):

    currtime = time.localtime()
    date = time.strftime('%Y-%m-%d',currtime)

    # extract params from config
    [dbserver,dbport,dbname,dbuser,dbpass] = get_db_config("db", ["dbserver","dbport","dbname", "dbuser", "dbpass"])
    [year_tab] = get_config("tables", ["year_tab"])

    #connecting to database
    conn, cursor = get_cursor()

    num_points = make_reqst_for_circle(conn,cursor,year_tab,lim_for,limit,from_time,period,circle,whom)
    dst_file_name = make_file_name(period, date, whom, result_dir,0)
    dst_file = os.path.join(result_dir,dst_file_name)
    write_to_kml(dbserver,dbport,dbname,dbuser,dbpass,dst_file,whom)
    drop_whom_table(conn,cursor,whom)

    close_conn(conn, cursor)

    return dst_file, num_points