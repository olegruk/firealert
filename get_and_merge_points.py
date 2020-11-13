# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------------
# Name:        get_and_merge_points
# Purpose:
# Author:      Chaus
# Created:     24.04.2019
#-------------------------------------------------------------------------------

import os, time
import psycopg2
import requests, shutil
import csv, pandas
from sqlalchemy import create_engine
from falogging import log, start_logging, stop_logging
from faservice import get_config, get_db_config, get_cursor, close_conn, get_path, send_to_telegram

#Создаем подкаталог по текущей дате
def MakeTodayDir(DateStr, aDir):
    log("Creating today dir %s..." %aDir)
    Dir_Today = aDir + DateStr
    if os.path.exists(Dir_Today):
        try:
            shutil.rmtree(Dir_Today)
        except OSError:
            log("Unable to remove %s" % Dir_Today)
    try:
        os.mkdir(Dir_Today)
        log("Created %s" % Dir_Today)
    except OSError:
        log("Unable to create %s" % Dir_Today)
    return Dir_Today

#def get session
def get_session(url):
    log("Requesting session %s..." %url)
    s = requests.session()
    headers = {
		'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
		'X-Requested-With': 'XMLHttpRequest',
		'Referer': url,
		'Pragma': 'no-cache',
		'Cache-Control': 'no-cache'}
    r = s.get(url, headers=headers)
    #r = s.get(url, headers=headers,verify=False)
    log("Session created")
    return(r)

#read file from site and save to csv
def read_csv_from_site(url,sourcepath):
    filereq = requests.get(url,stream = True)
    #filereq = requests.get(url,stream = True,verify=False)
    with open(sourcepath,"wb") as receive:
        shutil.copyfileobj(filereq.raw,receive)
        del filereq

#Процедура получения csv-файла точек
def GetPoints(pointset, dst_folder, aDate):
    log("Getting points for %s..." %pointset)
    [period] = get_config("NASA", ["load_period"])
    [src_url] = get_config("NASA", ["%s_src_%s"%(pointset,period)])
    dst_file = "%s_%s.csv"%(pointset,aDate)
    dst_file = os.path.join(dst_folder, dst_file)
    read_csv_from_site(src_url,dst_file)
    log("Download complete: %s" %dst_file)

#Процедура для конструктора
def getconn():
    # extract db params from config
    [dbserver,dbport,dbname,dbuser,dbpass] = get_db_config()
    c = psycopg2.connect(host=dbserver, port=dbport, dbname=dbname, user=dbuser, password=dbpass)
    return c

#Запись csv в таблицы
def write_to_db(engine, tablename, dataframe):
	# Use pandas and sqlalchemy to insert a dataframe into a database
    log("Writing points...")
    try:
        dataframe.to_sql(tablename,engine,index=False,if_exists=u'append',chunksize=1000)
        log("Done inserted source into postgres")
    except IOError as e:
        log("Error in inserting data into db:%s"%e)

#Загрузка точек из csv-файла в БД
def upload_points_to_db(cursor,src_folder,pointset,aDate):
    log("Upload points %s into postgres..." %pointset)
    engine = create_engine('postgresql+psycopg2://', creator=getconn)

    src_file = "%s_%s.csv"%(pointset,aDate)
    src_file = os.path.join(src_folder,src_file)
    dst_table = pointset + '_today'
    try:
        csv_src = pandas.read_csv(src_file)
        write_to_db(engine, dst_table, csv_src)
        cursor.execute("SELECT count(*) FROM %s"%(dst_table))
        points_count = cursor.fetchone()[0]
        log('%s rows added to db from %s'%(points_count, src_file))
    except IOError as e:
        log('Error download and add data %s' %e)
        points_count = 0
    return points_count

#Удаляем дневные таблицы
def drop_today_tables(conn,cursor, pointset):
    log("Dropping today tables for %s..." %pointset)
    today_tab = '%s_today'%(pointset)
    sql_stat = "DROP TABLE IF EXISTS %s"%(today_tab)
    try:
        cursor.execute(sql_stat)
        conn.commit()
        log("Tables dropped")
    except IOError as e:
        log('Error dropping table:$s'%e)

#Удаляем временные таблицы
def drop_temp_tables(conn,cursor, pointset):
    log("Dropping temp tables for %s..." %pointset)
    today_tab = pointset + '_today'
    today_tab_ru = pointset + '_today_ru'
    sql_stat_1 = "DROP TABLE IF EXISTS %s"%(today_tab)
    sql_stat_2 = "DROP TABLE IF EXISTS %s"%(today_tab_ru)
    try:
        cursor.execute(sql_stat_1)
        cursor.execute(sql_stat_2)
        conn.commit()
        log("Temp tables dropped.")
    except IOError as e:
        log('Error dropping temp tables:$s'%e)

#Добавляем геоинформацию
def add_geog_field(conn,cursor,pointset):
    log("Adding geog field for %s..." %pointset)
    src_tab = pointset + '_today'
    statements = (
        """
        ALTER TABLE %s
            ADD COLUMN geog geometry(POINT,4326)
        """%(src_tab),
        """
        UPDATE %s
            SET geog = ST_GeomFromText('POINT(' || longitude || ' ' || latitude || ')',4326)
        """%(src_tab),
        """
        CREATE INDEX %s_idx ON %s USING GIST (geog)
        """%(src_tab,src_tab)
		)
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
        conn.commit()
        log('Geometry added to %s'%(src_tab))
    except IOError as e:
        log('Error adding geometry:$s'%e)

#Делаем выборку точек по России
def make_tables_for_Russia(conn,cursor,pointset):
    log("Making table for Russia for %s..." %pointset)
    src_tab = pointset + '_today'
    dst_tab = pointset + '_today_ru'
    [outline] = get_config("regions", ["reg_russia"])
    statements = (
		"""
		DROP TABLE IF EXISTS %s
		"""%(dst_tab),
		"""
		CREATE TABLE %(d)s
			AS SELECT %(s)s.*, %(o)s.region
				FROM %(s)s, %(o)s
				WHERE ST_Intersects(%(o)s.geog, %(s)s.geog)
		"""%{'d':dst_tab, 's':src_tab, 'o':outline}
		)
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log('The table created:%s'%(dst_tab))
    except IOError as e:
        log('Error intersecting points with region:$s'%e)

#Создаем сводную таблицу
def make_common_table(conn,cursor,dst_tab,pointsets):
    log("Making common table...")
    statements = (
        """
		DROP TABLE IF EXISTS %s
		"""%(dst_tab),
		"""
		CREATE TABLE %s (
				gid SERIAL PRIMARY KEY,
                name VARCHAR(30),
                acq_date VARCHAR(10),
				acq_time VARCHAR(5),
                daynight VARCHAR(1),
				latitude NUMERIC,
				longitude NUMERIC,
                satellite VARCHAR(5),
                conf_modis INTEGER,
                conf_viirs VARCHAR(7),
                brightness NUMERIC,
                bright_t31 NUMERIC,
                bright_ti4 NUMERIC,
                bright_ti5 NUMERIC,
				scan NUMERIC,
				track NUMERIC,
				version VARCHAR(6),
				frp NUMERIC,
                region  VARCHAR(100),
				rating SMALLINT,
				critical SMALLINT,
                revision SMALLINT,
				peat_id VARCHAR(254),
				peat_district VARCHAR(254),
				peat_region VARCHAR(254),
                peat_area SMALLINT,
				peat_class SMALLINT,
				peat_fire SMALLINT,
                ident VARCHAR(45),
                date_time TIMESTAMP,
                geog GEOGRAPHY(POINT, 4326),
                marker VARCHAR(26),
                tech VARCHAR(256)
		)
		"""%(dst_tab)
	)
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log('The table created:%s'%(dst_tab))
    except IOError as e:
        log('Error creating table:$s'%e)

    loaded = 0

    for pointset in pointsets:
        src_tab = pointset + '_today_ru'

        cursor.execute("SELECT count(*) FROM %s"%(src_tab))
        points_count = cursor.fetchone()[0]
        if points_count == 0:
            continue

        ins_from_modis = """
            INSERT INTO %(d)s (acq_date,acq_time,daynight,latitude,longitude,satellite,conf_modis,brightness,bright_t31,scan,track,version,frp,region,geog)
                SELECT
                    %(s)s.acq_date,
                    %(s)s.acq_time,
                    %(s)s.daynight,
                    %(s)s.latitude,
                    %(s)s.longitude,
                    %(s)s.satellite,
                    %(s)s.confidence,
                    %(s)s.brightness,
                    %(s)s.bright_t31,
                    %(s)s.scan,
                    %(s)s.track,
                    %(s)s.version,
                    %(s)s.frp,
                    %(s)s.region,
                    %(s)s.geog
                FROM %(s)s
        """%{'d':dst_tab,'s':src_tab}

        ins_from_viirs = """
            INSERT INTO %(d)s (acq_date,acq_time,daynight,latitude,longitude,satellite,conf_viirs,bright_ti4,bright_ti5,scan,track,version,frp,region,geog)
                SELECT
                    %(s)s.acq_date,
                    %(s)s.acq_time,
                    %(s)s.daynight,
                    %(s)s.latitude,
                    %(s)s.longitude,
                    %(s)s.satellite,
                    %(s)s.confidence,
                    %(s)s.bright_ti4,
                    %(s)s.bright_ti5,
                    %(s)s.scan,
                    %(s)s.track,
                    %(s)s.version,
                    %(s)s.frp,
                    %(s)s.region,
                    %(s)s.geog
                FROM %(s)s
        """%{'d':dst_tab,'s':src_tab}

        try:
            if pointset in ['as_modis', 'eu_modis']:
                cursor.execute(ins_from_modis)
            elif pointset in ['as_viirs', 'eu_viirs', 'as_vnoaa', 'eu_vnoaa']:
                cursor.execute(ins_from_viirs)
            conn.commit()
            log('The data added:%s'%(src_tab))
        except IOError as e:
            log('Error adding data:$s'%e)

        loaded = loaded + 1
    return loaded

#Процедура оценки точек, попавших в буфер
def cost_point_in_buffers(conn,cursor,tablename):
    log("Costing points in buffers...")
    try:
        #Такая последовательность выборок позволяет выбирать точки по мере убывания их критичности
        parse_order = [(64,4), (64,3), (40,4), (32,4), (64,2), (40,3), (32,3), (20,4), (40,2), (16,4), (32,2), (64,1), (20,3), (16,3), (10,4), (20,2), (40,1), (8,4), (16,2), (32,1), (10,3), (6,4), (8,3), (5,4), (10,2), (20,1), (6,3), (4,4), (8,2), (16,1), (5,3), (3,4), (4,3), (6,2), (5,2), (10,1), (3,3), (2,4), (4,2), (8,1), (2,3), (3,2), (6,1), (5,1), (1,4), (2,2), (4,1), (1,3), (3,1), (1,2), (2,1), (1,1)]
        dist = ['buf_out','buf_far','buf_middle','buf_near','buf_core']

        for (fire_cost,rate) in parse_order:
            peat_db = dist[rate]
            set_rating = """
				UPDATE %(t)s SET
                    rating = %(r)s,
                    critical = %(f)s*%(r)s,
                    peat_id = %(p)s.unique_id,
                    peat_district = %(p)s.district,
                    peat_region = %(p)s.region,
                    peat_class = %(p)s.dry_indx,
                    peat_fire = %(p)s.burn_indx
                FROM %(p)s
                WHERE %(t)s.critical IS NULL AND %(p)s.burn_indx = %(f)s AND ST_Intersects(%(t)s.geog, %(p)s.geog)
			"""%{'t':tablename,'r':rate,'f':fire_cost,'p':peat_db}
            cursor.execute(set_rating)
            log('The rating %s setted to %s'%(rate, peat_db))
        set_zero_rating = """
			UPDATE %s SET
                rating = 0,
                critical = 0
            WHERE critical IS NULL
		"""%(tablename)
        cursor.execute(set_zero_rating)
        log('Zero rating setted')
        conn.commit()
    except IOError as e:
        log('Error costing points:$s'%e)

#Добавляем в результирующее поле Name = acq_date : gid : critical
def set_name_field(conn,cursor,tablename):
    log("Setting Name field...")

    #set_name = """
	#	UPDATE %s
	#		SET name = '[' || to_char(critical,'999') || '] : ' || acq_date || ' :' || to_char(gid,'99999')
    #"""%(tablename)

    #set_name = """
	#	UPDATE %s
	#		SET name = '(' || to_char(gid,'99999') || ') :' || acq_date || ' : [' || to_char(critical,'999') || ']'
    #"""%(tablename)

    set_name = """
		UPDATE %s
			SET name = to_char(gid,'9999999')
    """%(tablename)

    try:
        cursor.execute(set_name)
        conn.commit()
        log("A Name field setted")
    except IOError as e:
        log('Error setting points name:$s'%e)

#Создаем поле ident = acq_date:acq_time:latitude:longitude:satellite
def set_ident_field(conn,cursor,tablename):
    log("Setting Ident field...")
    set_ident = """
		UPDATE %s
			SET ident = acq_date || ':' || acq_time || ':' || to_char(latitude,'999.9999') || ':' || to_char(longitude,'999.9999') || ':' || satellite
    """%(tablename)
    try:
        cursor.execute(set_ident)
        conn.commit()
        log("A Ident field setted")
    except IOError as e:
        log('Error creating ident fields:$s'%e)

#Дополняем поле time ведущими нулями
def correct_time_field(conn,cursor,tablename):
    log("Correcting Time field...")
    set_ident = """
		UPDATE %s
			SET acq_time = left(lpad(acq_time, 4, '0'),2) || ':' || right(lpad(acq_time, 4, '0'),2)
    """%(tablename)
    try:
        cursor.execute(set_ident)
        conn.commit()
        log(" Time field corrected.")
    except IOError as e:
        log('Error correcting time fields:$s'%e)

#Устанавливаем значение поля date_time "acq_date acq_time"
def set_datetime_field(conn,cursor,tablename):
    log("Setting Date_time field...")
    set_datetime = """
		UPDATE %s
            SET date_time = TO_TIMESTAMP(acq_date || ' ' || acq_time,'YYYY-MM-DD HH24:MI')
    """%(tablename)
    try:
        cursor.execute(set_datetime)
        conn.commit()
        log("Date_time field setted.")
    except IOError as e:
        log('Error creating timestamp:$s'%e)

#Устанавливаем значение поля marker
def set_marker_field(conn,cursor,tablename,marker):
    log("Setting Marker field...")
    set_marker = """
		UPDATE %(s)s
            SET marker = '%(m)s'
    """%{'s': tablename, 'm': marker}
    try:
        cursor.execute(set_marker)
        conn.commit()
        log("Marker field setted.")
    except IOError as e:
        log('Error creating marker:$s'%e)


#Удаляем дубли в таблице
def del_duplicates(conn,cursor,tablename):
    log("Deleting duplicates in %s..." %(tablename))
    statements = (
        """
            CREATE TABLE %s_tmp AS SELECT DISTINCT ON (ident) * FROM %s
		"""%(tablename,tablename),
		"""
            DROP TABLE %s
		"""%(tablename),
        """
            ALTER TABLE %s_tmp RENAME TO %s
        """%(tablename,tablename)
	)
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log('The duplicates deleted in %s'%(tablename))
    except IOError as e:
        log('Error deleting duplicates:$s'%e)

#Повышаем оценку для групповых точек
def rise_multipoint_cost(conn,cursor,tablename,distance):
    log("Correcting cost for multipoints...")
    temp_tab = '%s_tmp1'%tablename
    clust_tab = '%s_clst'%tablename
    statements = (
        """
		DROP TABLE IF EXISTS %s
		"""%(temp_tab),
        """
        CREATE TABLE %(t)s
            AS SELECT
                ident,
                peat_id,
                peat_fire,
                rating,
                critical,
                revision,
                (ST_Transform(%(s)s.geog::geometry,3857)::geometry) AS geom
            FROM %(s)s
            WHERE rating > 0
        """%{'t':temp_tab,'s':tablename},
        """
		DROP TABLE IF EXISTS %s
		"""%(clust_tab),
        """
        CREATE TABLE %(c)s
            AS SELECT
                ST_NumGeometries(gc) as num,
                ST_Buffer(ST_ConvexHull(gc), %(b)s, 'quad_segs=8') AS buffer
            FROM (
                SELECT unnest(ST_ClusterWithin(geom, %(d)s)) AS gc
                FROM %(s)s)
                AS result
        """%{'c': clust_tab, 's': temp_tab, 'd': distance, 'b': 50},
         """
        DELETE FROM %s
            WHERE num < 2
        """%(clust_tab),
        """
		UPDATE %(t)s SET
            rating = rating + 1,
            critical = peat_fire*(rating+1),
            revision = 1
        FROM %(c)s
        WHERE ST_Intersects(%(t)s.geom, %(c)s.buffer)
        """%{'t':temp_tab,'c':clust_tab},
        """
		UPDATE %(s)s SET
            rating = %(t)s.rating,
            critical = %(t)s.critical
        FROM %(t)s
        WHERE %(t)s.ident = %(s)s.ident AND %(t)s.revision = 1
        """%{'t':temp_tab,'s':tablename},
        """
		DROP TABLE IF EXISTS %s
		"""%(temp_tab)
        #"""
		#DROP TABLE IF EXISTS %s
		#"""%(clust_tab)
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log('Cost corrected.')
    except IOError as e:
        log('Error correcting cost:$s'%e)

def check_tech_zones(conn, cursor, src_tab, tech_zones):
    log("Checking tech-zones...")
    sql_stat = """
        UPDATE %(s)s
		SET tech = %(o)s.name
        FROM %(o)s
        WHERE ST_Intersects(%(o)s.geog, %(s)s.geog)
		"""%{'s':src_tab, 'o':tech_zones}
    try:
        cursor.execute(sql_stat)
        conn.commit()
        log('Tech zones checked.')
    except IOError as e:
        log('Error intersecting points with tech-zones:$s'%e)

#Копирование данных в общую годичную таблицу
def copy_to_common_table(conn,cursor,today_tab, year_tab):
    log("Copying data into common table...")
    ins_string = """
        INSERT INTO %(y)s (name,acq_date,acq_time,daynight,latitude,longitude,satellite,conf_modis,conf_viirs,brightness,bright_t31,bright_ti4,bright_ti5,scan,track,version,frp,region,rating,critical,revision,peat_id,peat_district,peat_region,peat_area,peat_class,peat_fire,ident,date_time,geog,marker,tech)
            SELECT
                name,
                acq_date,
                acq_time,
                daynight,
                latitude,
                longitude,
                satellite,
                conf_modis,
                conf_viirs,
                brightness,
                bright_t31,
                bright_ti4,
                bright_ti5,
                scan,track,
                version,
                frp,
                region,
                rating,
                critical,
                revision,
                peat_id,
                peat_district,
                peat_region,
                peat_area,
                peat_class,
                peat_fire,
                ident,
                date_time,
                geog,
                marker,
                tech
            FROM %(t)s
                WHERE NOT EXISTS(
					SELECT ident FROM %(y)s
						WHERE %(t)s.ident = %(y)s.ident)
    """%{'y':year_tab, 't':today_tab}
    try:
        cursor.execute(ins_string)
        conn.commit()
        log('Data from %s added to common table %s'%(today_tab, year_tab))
    except IOError as e:
        log('Error addin points to common table:$s'%e)

def drop_today_table(conn,cursor,common_tab):
    log("Dropping today table...")
    try:
        cursor.execute("DROP TABLE IF EXISTS %s"%(common_tab))
        conn.commit()
        log("Today table dropped.")
    except IOError as e:
        log('Error dropping today table:$s'%e)


#Задачи
def get_and_merge_points_job():
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d',currtime)

    start_logging('get_and_merge_points.py')

    # extract db params from config
    [year_tab,common_tab,tech_zones] = get_config("tables", ["year_tab", "common_tab", "tech_zones"])
    [data_root,firms_folder] = get_config("path", ["data_root", "firms_folder"])
    [clst_dist] = get_config("clusters", ["cluster_dist"])
    [num_of_src] = get_config("sources", ["num_of_src"])
    [pointsets] = get_config("sources", ["src"])
    [url, chat_id] = get_config("telegramm", ["url", "tst_chat_id"])
    firms_path = get_path(data_root,firms_folder)

    loaded_set = []

    #connecting to database
    conn, cursor = get_cursor()

    for pointset in pointsets:
        #Получаем точки (скачиваем csv-файлы)
        GetPoints(pointset, firms_path, date)
        #Удаляем дневные таблицы
        drop_today_tables(conn,cursor,pointset)
        #Загружаем скачанные файлы в БД
        count = upload_points_to_db(cursor, firms_path, pointset,date)
        if count > 0:
            #Добавляем поле с геоданными
            add_geog_field(conn,cursor,pointset)
            #Выбираем точки на территории России
            make_tables_for_Russia(conn,cursor,pointset)
            loaded_set.append(pointset)
        else:
            msg = 'Zero-rows file: %s'%pointset
            send_to_telegram(url, chat_id, msg)

    #Собираем в единую таблицу
    loaded = make_common_table(conn,cursor,common_tab,loaded_set)

    if loaded < int(num_of_src):
        msg = 'Загружены данные из %s таблиц'%loaded
        send_to_telegram(url, chat_id, msg)

    #Приводим время к виду "чч:мм"
    correct_time_field(conn,cursor,common_tab)

    #Устанавливаем значение поля date_time "acq_date acq_time"
    set_datetime_field(conn,cursor,common_tab)

    #Устанавливаем значение поля marker
    marker = ''
    set_marker_field(conn,cursor,common_tab,marker)

    #Добавляем поле с уникальным идентификатором "acq_date:acq_time:latitude:longitude:satellite"
    set_ident_field(conn,cursor,common_tab)

    #Удаляем дубликаты строк
    del_duplicates(conn,cursor,common_tab)

    #Выделяем точки, попавшие в критичные буферы и пишем оценку
    cost_point_in_buffers(conn,cursor,common_tab)

    #Добавляем поле name вида [critical] : acq_date : gid
    set_name_field(conn,cursor,common_tab)

    #Повышаем оценку для групповых точек
    rise_multipoint_cost(conn,cursor,common_tab,clst_dist)

    #Проверка точек на попадание в слой техногена
    check_tech_zones(conn, cursor, common_tab, tech_zones)

    #Копирование данных в общую годичную таблицу
    copy_to_common_table(conn,cursor,common_tab, year_tab)

    #Удаляем дневные таблицы
    for pointset in pointsets:
        drop_temp_tables(conn,cursor,pointset)

    #Удаляем сводную таблицу за день
    drop_today_table(conn,cursor,common_tab)

    close_conn(conn, cursor)
    stop_logging('get_and_merge_points.py')

#main
if __name__ == "__main__":
    get_and_merge_points_job()
