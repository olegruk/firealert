#-------------------------------------------------------------------------------
# Name:        receive_from_dispatcher
# Purpose:
# Author:      Chaus
# Created:     23.03.2019
#-------------------------------------------------------------------------------

import os, time, sys
import logging
import psycopg2
from osgeo import ogr
import yadisk
import posixpath
from configparser import ConfigParser

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

#Создаем таблицу для загрузки от диспетчера
def make_table_for(conn,cursor,whom):
    statements = (
        """
		DROP TABLE IF EXISTS %s
		"""%(whom),
		"""
		CREATE TABLE %s (
                name VARCHAR(30),
                acq_date VARCHAR(10),
				acq_time VARCHAR(5),
                satellite VARCHAR(5),
                region  VARCHAR(100),
				rating SMALLINT,
				critical SMALLINT,
                revision VARCHAR(3),
				peat_id INTEGER,
				peat_district VARCHAR(254),
				peat_class SMALLINT,
				peat_fire SMALLINT,
                ident VARCHAR(45),
                geog GEOGRAPHY(POINT, 4326)
		)
		"""%(whom)
	)
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log(logfile, 'The table created:%s'%(whom))
    except IOError as e:
        log(logfile, 'Error creating table for data load:$s'%e)

def read_from_yadisk(from_dir,to_dir,file,yadisk_token):
    y = yadisk.YaDisk(token=yadisk_token)
    p = to_dir.split(to_dir)[1].strip(os.path.sep)
    dir_path = posixpath.join(from_dir, p)
    file_path = posixpath.join(dir_path, file)
    p_sys = p.replace("/", os.path.sep)
    in_path = os.path.join(to_dir, p_sys, file)
    try:
        y.download(file_path, in_path)
        log(logfile, 'Written to disk %s'%(in_path))
        y.remove(file_path)
        log(logfile, 'File  %s removed from ya-disk'%(file_path))
    except yadisk.exceptions.PathExistsError:
        log(logfile, 'Path not exist %s.'%(file_path))
        pass

def read_from_kml(src_dir,src_file, dst_tab,dbserver,dbport,dbname,dbuser,dbpass):
    src_path = os.path.join(src_dir,src_file)
    if os.path.isfile(src_path):
        command = """ogr2ogr -append -overwrite -t_srs "+init=epsg:4326" -f "PostgreSQL"  PG:"host=%(h)s user=%(u)s dbname=%(b)s password=%(w)s port=%(p)s" %(s)s -nln %(d)s"""%{'h':dbserver,'u':dbuser,'b':dbname,'w':dbpass,'p':dbport,'s':src_path,'d':dst_tab}
        os.system(command)
        log(logfile, 'A table %s is created from kml in %s'%(dst_tab,src_path))
        os.remove(src_path)
    else:
        log(logfile, 'No any kml in %s'%(src_path))

def clear_table(conn,cursor,src_tab):
    statements = (
        """
        ALTER TABLE %s
            DROP COLUMN IF EXISTS description
        """%(src_tab),
        """
        ALTER TABLE %s
            DROP COLUMN IF EXISTS timestamp
        """%(src_tab),
        """
        ALTER TABLE %s
            DROP COLUMN IF EXISTS begin
        """%(src_tab),
        """
        ALTER TABLE %s
            DROP COLUMN IF EXISTS "end"
        """%(src_tab),
        """
        ALTER TABLE %s
            DROP COLUMN IF EXISTS altitudemode
        """%(src_tab),
        """
        ALTER TABLE %s
            DROP COLUMN IF EXISTS tessellate
        """%(src_tab),
        """
        ALTER TABLE %s
            DROP COLUMN IF EXISTS extrude
        """%(src_tab),
        """
        ALTER TABLE %s
            DROP COLUMN IF EXISTS visibility
        """%(src_tab),
        """
        ALTER TABLE %s
            DROP COLUMN IF EXISTS draworder
        """%(src_tab),
        """
        ALTER TABLE %s
            DROP COLUMN IF EXISTS icon
        """%(src_tab)
		)
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
        conn.commit()
        log(logfile, 'Columns dropped from %s'%(src_tab))
    except IOError as e:
        log(logfile, 'Error deleting fields from kml-table:$s'%e)

def set_revision(conn,cursor,src_tab,year_tab):
    #Выделение поправки к оценке из имени через уделение старого имени из нового
    statements_a = (
        """
        ALTER TABLE %s
            ADD COLUMN revision SMALLINT
        """%(src_tab),
        """
        UPDATE %(s)s
            SET revision = %(s)s.peat_fire*(%(s)s.rating - SUBSTRING(REPLACE(%(s)s.name, %(y)s.name, '') FROM '\d')::SMALLINT)
            FROM  %(y)s
            WHERE %(s)s.ident = %(y)s.ident AND SUBSTR(REPLACE(%(s)s.name, %(y)s.name, ''),1,1) = '-'
        """%{'s':src_tab,'y':year_tab},
        """
        UPDATE %(s)s
            SET revision = %(s)s.peat_fire*(%(s)s.rating + SUBSTRING(REPLACE(%(s)s.name, %(y)s.name, '') FROM '\d')::SMALLINT)
            FROM  %(y)s
            WHERE %(s)s.ident = %(y)s.ident AND SUBSTR(REPLACE(%(s)s.name, %(y)s.name, ''),1,1) = '+'
        """%{'s':src_tab,'y':year_tab},
        """
        UPDATE %(s)s
            SET revision = 0
            FROM  %(y)s
            WHERE %(s)s.ident = %(y)s.ident AND SUBSTR(REPLACE(%(s)s.name, %(y)s.name, ''),2,1) = '0'
        """%{'s':src_tab,'y':year_tab}
		)
    #Выделение поправки к оценке из имени через откусывание последних двух символов
    statements_b = (
        """
        ALTER TABLE %s
            ADD COLUMN revision SMALLINT
        """%(src_tab),
        """
        UPDATE %(s)s
            SET revision = %(s)s.peat_fire*(%(s)s.rating - RIGHT(%(s)s.name, 1)::SMALLINT)
            FROM  %(y)s
            WHERE %(s)s.ident = %(y)s.ident AND LEFT(RIGHT(%(s)s.name, 2),1) = '-'
        """%{'s':src_tab,'y':year_tab},
        """
        UPDATE %(s)s
            SET revision = %(s)s.peat_fire*(%(s)s.rating + RIGHT(%(s)s.name, 1)::SMALLINT)
            FROM  %(y)s
            WHERE %(s)s.ident = %(y)s.ident AND LEFT(RIGHT(%(s)s.name, 2),1) = '+'
        """%{'s':src_tab,'y':year_tab},
        """
        UPDATE %(s)s
            SET revision = 0
            FROM  %(y)s
            WHERE %(s)s.ident = %(y)s.ident AND RIGHT(%(s)s.name, 1) = '0'
        """%{'s':src_tab,'y':year_tab}
		)
    try:
        for sql_stat in statements_b:
            cursor.execute(sql_stat)
        conn.commit()
        log(logfile, 'Revision field updated for %s'%(src_tab))
    except IOError as e:
        log(logfile, 'Error updating revision:$s'%e)

def copy_revision_to_total(conn,cursor,src_tab,year_tab):
    statements = (
        """
        UPDATE %(y)s
            SET revision = %(s)s.revision
            FROM  %(s)s
            WHERE %(s)s.ident = %(y)s.ident AND %(s)s.revision IS NOT NULL
        """%{'s':src_tab,'y':year_tab},
        """
        UPDATE %(y)s
            SET marker = 'u'
            FROM  %(s)s
            WHERE %(s)s.ident = %(y)s.ident AND %(s)s.revision IS NOT NULL
        """%{'s':src_tab,'y':year_tab}
    )

    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
        conn.commit()
        log(logfile, 'Revision copied to %s, marker updated.'%(year_tab))
    except IOError as e:
        log(logfile, 'Error while copy revision to common table:$s'%e)

def drop_temp_table(conn,cursor, a_table):
    try:
        cursor.execute("DROP TABLE IF EXISTS %s"%(a_table))
        conn.commit()
    except IOError as e:
        log(logfile, 'Error dropping table:$s'%e)

def receive_from_dispatcher_job():
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d',currtime)
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)

    logfile = get_log_file(date)
    log(logfile, '--------------------------------------------------------------------------------')
    log(logfile, 'Process [receive_from_dispatcher.py] started at %s'%(cdate))

     # extract db params from config
    [dbserver,dbport,dbname,dbuser,dbpass] = get_config(inifile, "db", ["dbserver","dbport","dbname", "dbuser", "dbpass"])
    [year_tab,disp_in_tab] = get_config(inifile, "tables", ["year_tab","disp_in_tab"])
    [data_root,temp_folder] = get_config(inifile, "path", ["data_root", "temp_folder"])
    [from_dir,yadisk_token] = get_config(inifile, "yadisk", ["yadisk_in_path", "yadisk_token"])

    #connecting to database
    conn = psycopg2.connect(dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()

    src_file = 'up.kml'
    temp_dir = get_path(data_root,temp_folder)

    make_table_for(conn,cursor,disp_in_tab)
    read_from_yadisk(from_dir, temp_dir, src_file,yadisk_token)
    read_from_kml(temp_dir,src_file,disp_in_tab,dbserver,dbport,dbname,dbuser,dbpass)
    clear_table(conn,cursor,disp_in_tab)
    set_revision(conn,cursor,disp_in_tab,year_tab)
    copy_revision_to_total(conn,cursor,disp_in_tab,year_tab)
    drop_temp_table(conn,cursor, disp_in_tab)

    cursor.close
    conn.close

    currtime = time.localtime()
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
    log(logfile, 'Process [receive_from_dispatcher.py] stopped at %s'%(cdate))


#main
if __name__ == "__main__":
    receive_from_dispatcher_job()
