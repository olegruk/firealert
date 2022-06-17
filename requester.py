#-------------------------------------------------------------------------------
# Name:        send_engine
# Purpose:
# Author:      Chaus
# Created:     13.05.2020
#-------------------------------------------------------------------------------

import os, time
from falogging import log
from faservice import get_config, get_cursor, close_conn, write_to_kml

#Создаем таблицу для выгрузки подписчикам
def make_reqst_table(conn,cursor,src_tab,crit_or_peat,limit, from_time, period, reg_list, whom,is_incremental):
    log("Creating table for subs_id:%s..." %whom)
    subs_tab = 'for_s%s' %str(whom)
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
    subs_tab = 'for_s%s' %str(whom)
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
    [year_tab] = get_config("tables", ["year_tab"])

    #connecting to database
    conn, cursor = get_cursor()

    num_points = make_reqst_table(conn,cursor,year_tab,lim_for,limit,from_time,period,regions,whom, False)
    dst_file_name = make_file_name(period, date, whom, result_dir,0)
    dst_file = os.path.join(result_dir,dst_file_name)
    write_to_kml(dst_file,whom)
    drop_whom_table(conn,cursor,whom)

    close_conn(conn, cursor)

    return dst_file, num_points

def request_for_circle(whom, lim_for, limit, from_time, period, circle, result_dir):

    currtime = time.localtime()
    date = time.strftime('%Y-%m-%d',currtime)

    # extract params from config
    [year_tab] = get_config("tables", ["year_tab"])

    #connecting to database
    conn, cursor = get_cursor()

    num_points = make_reqst_for_circle(conn,cursor,year_tab,lim_for,limit,from_time,period,circle,whom)
    dst_file_name = make_file_name(period, date, whom, result_dir,0)
    dst_file = os.path.join(result_dir,dst_file_name)
    write_to_kml(dst_file,whom)
    drop_whom_table(conn,cursor,whom)

    close_conn(conn, cursor)

    return dst_file, num_points

def check_reg_stat(reg, period, critical):
    log("Getting statistic for %s..."%(reg))
    # extract params from config
    [year_tab] = get_config("tables", ["year_tab"])
    #connecting to database
    conn, cursor = get_cursor()

    statements = (
        """
        SELECT count(*) FROM
            (SELECT name
            FROM %(y)s
            WHERE date_time >= TIMESTAMP 'today' - INTERVAL '%(p)s' AND date_time < TIMESTAMP 'today' AND critical >= %(c)s AND region = '%(r)s') as critical_sel
        """%{'y':year_tab,'p':period,'c':critical,'r':reg},
        """
        SELECT count(*) FROM
            (SELECT name
            FROM %(y)s
            WHERE date_time >= TIMESTAMP 'today' - INTERVAL '%(p)s' AND date_time < TIMESTAMP 'today' AND region = '%(r)s') as all_sel
        """%{'y':year_tab,'p':period,'r':reg}
        )
    try:
        cursor.execute(statements[0])
        critical_cnt = cursor.fetchone()[0]
        cursor.execute(statements[1])
        all_cnt = cursor.fetchone()[0]
        log('Finished for:%(r)s. All - %(a)s, critical - %(c)s'%{'r':reg, 'a':all_cnt,'c':critical_cnt})
    except IOError as e:
        log('Error getting statistic for region:$s'%e)

    close_conn(conn, cursor)

    return critical_cnt, all_cnt

def make_tlg_stat_msg(reg_list, period, limit):
    full_cnt = 0
    full_cr_cnt = 0
    msg = 'Количество точек:'
    for reg in reg_list:
        critical_cnt, all_cnt = check_reg_stat(reg, period, limit)
        if all_cnt > 0:
            msg = msg + '\r\n%(r)s: %(a)s'%{'r':reg,'a':all_cnt}
            if critical_cnt > 0:
                msg = msg + '\r\nкритичных: %(c)s'%{'c':critical_cnt}
        full_cnt = full_cnt + all_cnt
        full_cr_cnt = full_cr_cnt + critical_cnt
    if full_cnt == 0:
        msg = 'Нет новых точек.'
    return msg

def check_zone_stat(zone, period):
    log("Getting statistic for %s..."%(zone))
    # extract params from config
    [year_tab] = get_config("tables", ["year_tab"])
    #connecting to database
    conn, cursor = get_cursor()
    currtime = time.localtime()
    zone_time = time.strftime('%H',currtime)

    statements = (
        """
        SELECT count(*) FROM
            (SELECT name
            FROM %(y)s
            WHERE date_time >= TIMESTAMP 'now' - INTERVAL '%(p)s' AND vip_zone = '%(z)s' AND (vip_time IS NULL OR vip_time = '%(t)s')) as all_sel
        """%{'y':year_tab,'p':period,'z':zone,'t':zone_time},
        """
        UPDATE %(y)s SET
            vip_time = '%(t)s'
        WHERE date_time >= TIMESTAMP 'now' - INTERVAL '%(p)s' AND vip_zone = '%(z)s' AND vip_time IS NULL
        """%{'y':year_tab,'p':period,'z':zone,'t':zone_time}
        )

    try:
        cursor.execute(statements[0])
        all_cnt = cursor.fetchone()[0]
        cursor.execute(statements[1])
        log('Finished for:%(z)s. Points: %(p)s'%{'z':zone, 'p':all_cnt})
    except IOError as e:
        log('Error getting statistic for zone:$s'%e)

    close_conn(conn, cursor)

    return all_cnt

def make_zone_stat_msg(zone_list, period):
    full_cnt = 0
    msg = 'Новые точки в зонах особого внимания:'
    for zone in zone_list:
        all_cnt = check_zone_stat(zone, period)
        if all_cnt > 0:
            msg = msg + '\r\n%(z)s: %(a)s'%{'z':zone,'a':all_cnt}
        full_cnt = full_cnt + all_cnt
    if full_cnt == 0:
        msg = ''
    return msg

def check_oopt_stat(oopt_id, period):
    log("Getting statistic for OOPT %s..."%(oopt_id))
    # extract params from config
    [year_tab] = get_config("tables", ["year_tab"])
    #connecting to database
    conn, cursor = get_cursor()
    currtime = time.localtime()
    oopt_time = time.strftime('%H',currtime)

    statements = (
        """
        SELECT count(*) FROM
            (SELECT name
            FROM %(y)s
            WHERE date_time >= TIMESTAMP 'now' - INTERVAL '%(p)s' AND oopt_id = '%(o)s' AND (oopt_time IS NULL OR oopt_time = '%(t)s')) as all_sel
        """%{'y':year_tab,'p':period,'o':oopt_id,'t':oopt_time},
        """
        UPDATE %(y)s SET
            oopt_time = '%(t)s'
        WHERE date_time >= TIMESTAMP 'now' - INTERVAL '%(p)s' AND oopt_id = '%(o)s' AND oopt_time IS NULL
        """%{'y':year_tab,'p':period,'o':oopt_id,'t':oopt_time}
        )

    try:
        cursor.execute(statements[0])
        all_cnt = cursor.fetchone()[0]
        cursor.execute(statements[1])
        log('Finished for:%(o)s. Points: %(p)s'%{'o':oopt_id, 'p':all_cnt})
    except IOError as e:
        log('Error getting statistic for oopt:$s'%e)

    close_conn(conn, cursor)

    return all_cnt

def check_oopt_buf_stat(oopt_id, period):
    log("Getting statistic for OOPT buffers %s..."%(oopt_id))
    # extract params from config
    [year_tab] = get_config("tables", ["year_tab"])
    #connecting to database
    conn, cursor = get_cursor()
    currtime = time.localtime()
    oopt_time = time.strftime('%H',currtime)

    statements = (
        """
        SELECT count(*) FROM
            (SELECT name
            FROM %(y)s
            WHERE date_time >= TIMESTAMP 'now' - INTERVAL '%(p)s' AND oopt_buf_id = '%(o)s' AND (oopt_time IS NULL OR oopt_time = '%(t)s')) as all_sel
        """%{'y':year_tab,'p':period,'o':oopt_id,'t':oopt_time},
        """
        UPDATE %(y)s SET
            oopt_time = '%(t)s'
        WHERE date_time >= TIMESTAMP 'now' - INTERVAL '%(p)s' AND oopt_buf_id = '%(o)s' AND oopt_time IS NULL
        """%{'y':year_tab,'p':period,'o':oopt_id,'t':oopt_time}
        )

    try:
        cursor.execute(statements[0])
        all_cnt = cursor.fetchone()[0]
        cursor.execute(statements[1])
        log('Finished for:%(o)s. Points: %(p)s'%{'o':oopt_id, 'p':all_cnt})
    except IOError as e:
        log('Error getting statistic for oopt buffers:$s'%e)

    close_conn(conn, cursor)

    return all_cnt

def make_oopt_stat_msg(oopt_list, period):
    full_cnt = 0
    msg = 'Новые точки в ООПТ:'
    for oopt in oopt_list:
        all_cnt = check_oopt_stat(oopt[0], period)
        if all_cnt > 0:
            msg = msg + '\r\n%(r)s - %(o)s: %(c)s'%{'r':oopt[1],'o':oopt[2],'c':all_cnt}
        full_cnt = full_cnt + all_cnt
    if full_cnt == 0:
        msg = ''
    return msg

def make_oopt_buf_stat_msg(oopt_list, period):
    full_cnt = 0
    msg = 'Новые точки в буферных зонах ООПТ:'
    for oopt in oopt_list:
        all_cnt = check_oopt_buf_stat(oopt[0], period)
        if all_cnt > 0:
            msg = msg + '\r\n%(r)s - %(o)s: %(c)s'%{'r':oopt[1],'o':oopt[2],'c':all_cnt}
        full_cnt = full_cnt + all_cnt
    if full_cnt == 0:
        msg = ''
    return msg

def make_smf_stat_msg(reg_list, period, limit):
    full_cnt = 0
    full_cr_cnt = 0
    smf_msg = 'Количество точек:\r\n\r\n[table]'
    smf_msg = smf_msg + '\r\n[tr][td][b]Регион[/b][/td][td]   [/td][td][b]Всего точек   [/b][/td][td][b]Критичных точек[/b][/td][/tr]'
    for reg in reg_list:
        critical_cnt, all_cnt = check_reg_stat(reg, period, limit)
        smf_msg = smf_msg + '\r\n[tr][td]%(r)s[/td][td]   [/td][td][center]%(a)s[/center][/td][td][center]%(c)s[/center][/td][/tr]'%{'r':reg,'a':all_cnt, 'c':critical_cnt}
        full_cnt = full_cnt + all_cnt
        full_cr_cnt = full_cr_cnt + critical_cnt
    smf_msg = smf_msg + '\r\n[tr][td][b]Всего:[/b][/td][td]   [/td][td][center][b]%(a)s[/b][/center][/td][td][center][b]%(c)s[/b][/center][/td][/tr]'%{'a':full_cnt, 'c':full_cr_cnt}
    smf_msg = smf_msg + '\r\n[/table]'
    if full_cnt == 0:
        smf_msg = 'Нет новых точек.'
    return smf_msg

def new_alerts(period, cur_date):
    log("Adding alerts...")

    # extract params from config
    [alert_tab,clust_view] = get_config("peats_stat", ["alert_tab", "cluster_view"])
    #connecting to database
    conn, cursor = get_cursor()

    statements = (
        """
        INSERT INTO %(a)s (object_id, alert_date, point_count, satellite_base, cluster)
            SELECT
                peat_id,
                date_time,
  		        point_count,
                'https://apps.sentinel-hub.com/eo-browser/?zoom=14&lat=' || ST_Y(ST_Transform(ST_Centroid(buffer)::geometry,4326)::geometry) || '&lng=' || ST_X(ST_Transform(ST_Centroid(buffer)::geometry,4326)::geometry) || '&themeId=DEFAULT-THEME',
                buffer
            FROM %(t)s
            WHERE date_time >= (TIMESTAMP 'today' - INTERVAL '%(p)s') AND date_time < TIMESTAMP 'today'
        """%{'a':alert_tab,'t':clust_view,'p':period},
        """
        UPDATE %(a)s SET
            alert_date = '%(d)s',
            source = 'Робот'
        WHERE alert_date >= (TIMESTAMP 'today' - INTERVAL '%(p)s') AND alert_date < TIMESTAMP 'today' AND source IS NULL
        """%{'a':alert_tab,'d':cur_date, 'p':period}
        )

    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log('Adding alerts finished.')
    except IOError as e:
        log('Error adding alerts:$s'%e)

    close_conn(conn, cursor)

    #cursor.execute("SELECT count(*) FROM %s"%(subs_tab))
    #return cursor.fetchone()[0]

def check_peats_stat(reglist, cur_date):
    log("Getting peats statistic...")
    [year_tab] = get_config("tables", ["year_tab"])
    [period, critical,alert_tab] = get_config("peats_stat", ["period", "critical_limit", "alert_tab"])

    conn, cursor = get_cursor()

    statements = (
        """
        INSERT INTO %(a)s (object_id, point_count)
            SELECT
                peat_id,
  		        COUNT(*) AS num
            FROM %(y)s
            WHERE date_time >= NOW() - INTERVAL '%(p)s' AND (critical >= %(c)s OR revision >= %(c)s) AND region IN %(r)s
            GROUP BY peat_id
            ORDER BY num DESC
        """%{'a':alert_tab,'y':year_tab,'p':period,'c':critical,'r':reglist},
        """
        UPDATE %(a)s SET
            alert_date = '%(d)s',
            source = 'ДМ'
        WHERE alert_date IS NULL
        """%{'a':alert_tab,'d':cur_date}
        )

    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log('TGetting peats statistic finished.')
    except IOError as e:
        log('Error getting peats statistic:$s'%e)

    close_conn(conn, cursor)

    #cursor.execute("SELECT count(*) FROM %s"%(subs_tab))
    #return cursor.fetchone()[0]

def check_vip_zones(outline, period):
    log("Checking VIP-zones...")

    [year_tab] = get_config("tables", ["year_tab"])
    dst_tab = year_tab + '_vip'

    conn, cursor = get_cursor()

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
        log('The table created:%s'%(dst_tab))
    except IOError as e:
        log('Error intersecting points with region:$s'%e)
    cursor.execute("SELECT count(*) FROM %s"%(dst_tab))
    points_count = cursor.fetchone()[0]
    #cursor.execute("SELECT DISTINCT zone_name FROM %s"%(dst_tab))
    cursor.execute("SELECT zone_name, COUNT(*) FROM %s GROUP BY zone_name"%(dst_tab))
    zones = cursor.fetchall()

    close_conn(conn, cursor)

    return points_count, zones

def get_oopt_for_region(reglist):
    log("Making OOPT list for regions...")
    [oopt_zones] = get_config("tables", ["oopt_zones"])
    conn, cursor = get_cursor()
    cursor.execute("SELECT fid, region, name FROM %(t)s WHERE region IN (%(r)s)"%{'t':oopt_zones, 'r':reglist})
    oopt_list = cursor.fetchall()
    return oopt_list

def get_oopt_for_ids(oopt_ids):
    log("Making OOPT list for ids...")
    [oopt_zones] = get_config("tables", ["oopt_zones"])
    conn, cursor = get_cursor()
    cursor.execute("SELECT fid, region, name  FROM %(t)s WHERE fid IN (%(i)s)"%{'t':oopt_zones, 'i':oopt_ids})
    oopt_list = cursor.fetchall()
    return oopt_list