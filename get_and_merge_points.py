"""
Main firealert robot part.

Started via crontab: '05 * * * * get_and_merge_points.py'

Get firepoints from NASA servers? analyze it and add it to firealert db.

Created:     24.04.2019

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/

"""

import os
import time
import psycopg2
import requests
import shutil
from requests.adapters import HTTPAdapter
from requests.exceptions import (
    ConnectionError,
    HTTPError,
    RequestException)
import csv
import pandas
from sqlalchemy import create_engine
from faservice import (
    get_config,
    get_db_config,
    get_cursor,
    close_conn,
    get_path,
    send_to_telegram)
from mylogger import init_logger

logger = init_logger()


def MakeTodayDir(DateStr, aDir):
    """Create a subdir, based on current date."""
    logger.info(f"Creating today dir {aDir}...")
    Dir_Today = aDir + DateStr
    if os.path.exists(Dir_Today):
        try:
            shutil.rmtree(Dir_Today)
        except OSError:
            logger.error("Unable to remove %s" % Dir_Today)
    try:
        os.mkdir(Dir_Today)
        logger.info("Created %s" % Dir_Today)
    except OSError:
        logger.error("Unable to create %s" % Dir_Today)
    return Dir_Today


def get_session(url):
    """Get session for download firepoints."""
    logger.info(f"Requesting session {url}...")
    s = requests.session()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": url,
        "Pragma": "no-cache",
        "Cache-Control": "no-cache"}
    r = s.get(url, headers=headers)
    # r = s.get(url, headers=headers, verify=False)
    logger.info("Session created.")
    return r


def read_csv_from_site(url, sourcepath):
    """Read file from site and save to csv."""
    try:
        filereq = requests.get(url, stream=True)
        filereq.raise_for_status()
    except HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        errcode = 2
    except Exception as err:
        logger.error(f"Other error occurred: {err}")
        errcode = 3
    else:
        logger.info(f"Get receive status code {filereq.status_code}")
        # filereq = requests.get(url, stream=True, verify=False)
        with open(sourcepath, "wb") as receive:
            shutil.copyfileobj(filereq.raw, receive)
            del filereq
        errcode = 0
    return errcode


def read_csv_from_site_with_retries(url, sourcepath):
    """Read file from site with retries and save to csv."""
    nasa_adapter = HTTPAdapter(max_retries=3)
    session = requests.Session()
    session.mount(url, nasa_adapter)
    try:
        filereq = session.get(url, stream=True)
    except ConnectionError as conn_err:
        logger.error(f"Connection error occurred: {conn_err}")
        errcode = 1
    except HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        errcode = 2
    except RequestException as req_err:
        logger.error(f"Request error occurred: {req_err}")
        errcode = 3
    except Exception as err:
        logger.error(f"Other error occurred: {err}")
        errcode = 4
    else:
        logger.info(f"Get receive status code {filereq.status_code}")
        # filereq = requests.get(url, stream=True, verify=False)
        with open(sourcepath, "wb") as receive:
            shutil.copyfileobj(filereq.raw, receive)
            del filereq
            csv_src_str = str(receive)[0:255]
            if "<html>" in csv_src_str:
                errcode = 5
                logger.warning(f"Html in loaded csv: \n{csv_src_str}")
            else:
                errcode = 0
    return errcode


def GetPoints(pointset, dst_folder, aDate):
    """Get csv-file from NASA site."""
    logger.info(f"Getting points for {pointset}...")
    [period] = get_config("NASA", ["load_period"])
    [src_url] = get_config("NASA", [f"{pointset}_src_{period}"])
    dst_file = f"{pointset}_{aDate}.csv"
    dst_file = os.path.join(dst_folder, dst_file)
    errcode = read_csv_from_site(src_url, dst_file)
    if errcode == 0:
        logger.info(f"Download complete: {dst_file}")
    else:
        if os.path.exists(dst_file):
            try:
                os.remove(dst_file)
            except OSError as err:
                logger.error(f"Unable to remove file: {dst_file}.\n"
                             f"Error {err}")
        logger.warning(f"Not downloaded: {dst_file}.")
    return errcode


def GetPoints_with_retries(pointset, dst_folder, aDate):
    """Get csv-file from NASA site with retries."""
    logger.info(f"Getting points for {pointset}...")
    [period] = get_config("NASA", ["load_period"])
    [src_url] = get_config("NASA", [f"{pointset}_src_{period}"])
    [src_url2] = get_config("NASA", [f"{pointset}_src2_{period}"])
    dst_file = f"{pointset}_{aDate}.csv"
    dst_file = os.path.join(dst_folder, dst_file)
    errcode = read_csv_from_site_with_retries(src_url, dst_file)
    if errcode == 0:
        logger.info(f"Download complete: {dst_file}.")
    else:
        errcode = read_csv_from_site_with_retries(src_url2, dst_file)
        if errcode == 0:
            logger.info(f"Download complete: {dst_file}.")
        else:
            if os.path.exists(dst_file):
                try:
                    os.remove(dst_file)
                except OSError as err:
                    logger.error(f"Unable to remove file: {dst_file}.\n"
                                 f"Error {err}")
            logger.warning(f"Not downloaded: {dst_file}.")
    return errcode


def getconn():
    """Create psycopg2 connection (for constructor)."""
    [dbserver, dbport, dbname, dbuser, dbpass] = get_db_config()
    conn = psycopg2.connect(host=dbserver,
                            port=dbport,
                            dbname=dbname,
                            user=dbuser,
                            password=dbpass)
    return conn


def write_to_db(engine, tablename, dataframe):
    """Write pandas dataframe (from csv) into table."""
    logger.info("Writing points...")
    try:
        dataframe.to_sql(tablename,
                         engine,
                         index=False,
                         if_exists=u"append",
                         chunksize=1000)
        logger.info("Done inserted source into postgres")
    except psycopg2.Error as err:
        logger.error(f"Error in inserting data into db: {err}")


def upload_points_to_db(cursor, src_folder, pointset, aDate):
    """Upload points from csv-file into database."""
    logger.info(f"Upload points {pointset} into postgres...")
    engine = create_engine("postgresql+psycopg2://", creator=getconn)

    src_file = f"{pointset}_{aDate}.csv"
    src_file = os.path.join(src_folder, src_file)
    dst_table = f"{pointset}_today"
    try:
        csv_src = pandas.read_csv(src_file)
        csv_src_str = str(csv_src)[0:255]
        # logger.warning(f"Loaded text: \n>>>{ind}")
        if "<html>" in csv_src_str:
            points_count = 0
            logger.warning(f"Html in loaded csv: \n{csv_src_str}")
        else:
            write_to_db(engine, dst_table, csv_src)
            cursor.execute(f"SELECT count(*) FROM {dst_table}")
            points_count = cursor.fetchone()[0]
            logger.info(f"{points_count} rows added to db from {src_file}")
    except psycopg2.Error as err:
        logger.error("Error download and add data {err}")
        points_count = 0
    return points_count


def drop_today_tables(conn, cursor, pointset):
    """Drop temporary today tables."""
    logger.info(f"Dropping today tables for {pointset}...")
    today_tab = f"{pointset}_today"
    sql_stat = f"DROP TABLE IF EXISTS {today_tab}"
    try:
        cursor.execute(sql_stat)
        conn.commit()
        logger.info("Tables dropped")
    except psycopg2.Error as err:
        logger.error(f"Error dropping table: {err}")


def drop_temp_tables(conn, cursor, pointset):
    """Drop temporary tables."""
    logger.info(f"Dropping temp tables for {pointset}...")
    today_tab = f"{pointset}_today"
    today_tab_ru = f"{pointset}_today_ru"
    sql_stat_1 = f"DROP TABLE IF EXISTS {today_tab}"
    sql_stat_2 = f"DROP TABLE IF EXISTS {today_tab_ru}"
    try:
        cursor.execute(sql_stat_1)
        cursor.execute(sql_stat_2)
        conn.commit()
        logger.info("Temp tables dropped.")
    except psycopg2.Error as err:
        logger.error(f"Error dropping temp tables: {err}")


def add_geog_field(conn, cursor, pointset):
    """Add geog field to today-table."""
    logger.info(f"Adding geog field for {pointset}...")
    src_tab = f"{pointset}_today"
    statements = (
        f"""
        ALTER TABLE {src_tab}
            ADD COLUMN geog GEOGRAPHY(POINT, 4326)
        """,
        f"""
        UPDATE {src_tab}
            SET geog = ST_GeomFromText(
                        'POINT(' || longitude || ' ' || latitude || ')',4326)
        """,
        f"""
        CREATE INDEX {src_tab}_idx ON {src_tab} USING GIST (geog)
        """
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
        conn.commit()
        logger.info(f"Geometry added to {src_tab}.")
    except psycopg2.Error as err:
        logger.error(f"Error adding geometry: {err}")


def make_tables_for_mon_area(conn, cursor, pointset):
    """Make firepoints subset for monitored area."""
    logger.info(f"Making table for monitored area for {pointset}...")
    src_tab = f"{pointset}_today"
    dst_tab = f"{pointset}_today_ru"
    [outline] = get_config("regions", ["monitored_area"])
    statements = (
        f"""
        DROP TABLE IF EXISTS {dst_tab}
        """,
        f"""
        CREATE TABLE {dst_tab} AS
            SELECT {src_tab}.*, {outline}.region, {outline}.country
            FROM {src_tab}, {outline}
            WHERE
                ST_Intersects({outline}.geog, {src_tab}.geog)
        """
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        logger.info(f"The table created: {dst_tab}.")
    except psycopg2.Error as err:
        logger.error(f"Error intersecting points with region: {err}")


def make_common_table(conn, cursor, dst_tab, pointsets):
    """Merge points from source tables into common table."""
    logger.info("Making common table...")
    statements = (
        f"""
        DROP TABLE IF EXISTS {dst_tab}
        """,
        f"""
        CREATE TABLE {dst_tab} (
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
                peat_un_id VARCHAR(254),
                peat_district VARCHAR(254),
                peat_region VARCHAR(254),
                peat_area SMALLINT,
                peat_class SMALLINT,
                peat_fire SMALLINT,
                ident VARCHAR(45),
                date_time TIMESTAMP,
                geog GEOGRAPHY(POINT, 4326),
                marker VARCHAR(26),
                tech VARCHAR(254),
                vip_zone VARCHAR(254),
                oopt VARCHAR(254),
                oopt_id INTEGER,
                country VARCHAR(50),
                peat_id INTEGER,
                ctrl_id INTEGER,
                safe_id INTEGER,
                attn_id INTEGER,
                tech_id INTEGER,
                peat_buf_id INTEGER,
                oopt_buf_id INTEGER,
                ctrl_buf_id INTEGER,
                safe_buf_id INTEGER,
                attn_buf_id INTEGER,
                distance INTEGER,
                zone_id INTEGER,
                l_code INTEGER,
                peat_dist INTEGER,
                attn_dist INTEGER,
                oopt_dist INTEGER
        )
        """
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        logger.info(f"The table created: {dst_tab}.")
    except psycopg2.Error as err:
        logger.error(f"Error creating table: {err}")

    loaded = 0

    for pointset in pointsets:
        src_tab = f"{pointset}_today_ru"

        cursor.execute(f"SELECT count(*) FROM {src_tab}")
        points_count = cursor.fetchone()[0]
        if points_count == 0:
            continue

        ins_from_modis = f"""
            INSERT INTO {dst_tab} (acq_date,
                                   acq_time,
                                   daynight,
                                   latitude,
                                   longitude,
                                   satellite,
                                   conf_modis,
                                   brightness,
                                   bright_t31,
                                   scan,
                                   track,
                                   version,
                                   frp,
                                   region,
                                   country,
                                   geog)
                SELECT
                    {src_tab}.acq_date,
                    {src_tab}.acq_time,
                    {src_tab}.daynight,
                    {src_tab}.latitude,
                    {src_tab}.longitude,
                    {src_tab}.satellite,
                    {src_tab}.confidence,
                    {src_tab}.brightness,
                    {src_tab}.bright_t31,
                    {src_tab}.scan,
                    {src_tab}.track,
                    {src_tab}.version,
                    {src_tab}.frp,
                    {src_tab}.region,
                    {src_tab}.country,
                    {src_tab}.geog
                FROM {src_tab}
        """

        ins_from_viirs = f"""
            INSERT INTO {dst_tab} (acq_date,
                               acq_time,
                               daynight,
                               latitude,
                               longitude,
                               satellite,
                               conf_viirs,
                               bright_ti4,
                               bright_ti5,
                               scan,
                               track,
                               version,
                               frp,
                               region,
                               country,
                               geog)
                SELECT
                    {src_tab}.acq_date,
                    {src_tab}.acq_time,
                    {src_tab}.daynight,
                    {src_tab}.latitude,
                    {src_tab}.longitude,
                    {src_tab}.satellite,
                    {src_tab}.confidence,
                    {src_tab}.bright_ti4,
                    {src_tab}.bright_ti5,
                    {src_tab}.scan,
                    {src_tab}.track,
                    {src_tab}.version,
                    {src_tab}.frp,
                    {src_tab}.region,
                    {src_tab}.country,
                    {src_tab}.geog
                FROM {src_tab}
        """

        try:
            if pointset in ["as_modis", "eu_modis"]:
                cursor.execute(ins_from_modis)
            elif pointset in ["as_viirs", "eu_viirs", "as_vnoaa", "eu_vnoaa"]:
                cursor.execute(ins_from_viirs)
            conn.commit()
            cursor.execute(f"SELECT count(*) FROM {dst_tab}")
            points_count = cursor.fetchone()[0]
            logger.info(f"The {points_count} rows of data added: {src_tab}")
        except psycopg2.Error as err:
            logger.error(f"Error adding data: {err}")

        loaded = loaded + 1
    return loaded


def cost_point_in_buffers(conn, cursor, tablename):
    """Cost points located in peat buffers."""
    logger.info("Costing points in buffers...")
    try:
        # Такая последовательность выборок позволяет выбирать точки
        # по мере убывания их критичности
        parse_order = [(64, 4), (64, 3), (40, 4), (32, 4), (64, 2), (40, 3),
                       (32, 3), (20, 4), (40, 2), (16, 4), (32, 2), (64, 1),
                       (20, 3), (16, 3), (10, 4), (20, 2), (40, 1), (8, 4),
                       (16, 2), (32, 1), (10, 3), (6, 4), (8, 3), (5, 4),
                       (10, 2), (20, 1), (6, 3), (4, 4), (8, 2), (16, 1),
                       (5, 3), (3, 4), (4, 3), (6, 2), (5, 2), (10, 1),
                       (3, 3), (2, 4), (4, 2), (8, 1), (2, 3), (3, 2),
                       (6, 1), (5, 1), (1, 4), (2, 2), (4, 1), (1, 3),
                       (3, 1), (1, 2), (2, 1), (1, 1)]
        dist = ["buf_out", "buf_far", "buf_middle", "buf_near", "buf_core"]

        for (fire_cost, rate) in parse_order:
            peat_db = dist[rate]
            set_rating = f"""
                UPDATE {tablename}
                SET
                    rating = {rate},
                    critical = {fire_cost}*{rate},
                    peat_un_id = {peat_db}.unique_id,
                    peat_district = {peat_db}.district,
                    peat_region = {peat_db}.region,
                    peat_class = {peat_db}.dry_indx,
                    peat_fire = {peat_db}.burn_indx
                FROM {peat_db}
                WHERE
                    {tablename}.critical IS NULL
                    AND {peat_db}.burn_indx = {fire_cost}
                    AND ST_Intersects({tablename}.geog, {peat_db}.geog)
            """
            cursor.execute(set_rating)
            logger.info(f"The rating {rate} setted to {peat_db}.")
        set_zero_rating = f"""
            UPDATE {tablename}
            SET
                rating = 0,
                critical = 0
            WHERE
                critical IS NULL
        """
        cursor.execute(set_zero_rating)
        logger.info("Zero rating setted.")
        conn.commit()
    except psycopg2.Error as err:
        logger.error(f"Error costing points: {err}")


def set_name_field(conn, cursor, tablename):
    """Add a 'name' field (name = acq_date : gid : critical)."""
    logger.info("Setting 'name' field...")

    # set_name = f"""
    #    UPDATE {tablename}
    #    SET name = '[' \
    #               || to_char(critical,'999') \
    #               || '] : ' \
    #               || acq_date \
    #               || ' :' \
    #               || to_char(gid,'99999')
    # """

    # set_name = f"""
    #    UPDATE {tablename}
    #    SET name = '(' \
    #               || to_char(gid,'99999') \
    #               || ') :' \
    #               || acq_date \
    #               || ' : [' \
    #               || to_char(critical,'999') \
    #               || ']'
    # """

    set_name = f"""
        UPDATE {tablename}
        SET name = to_char(gid,'9999999')
    """

    try:
        cursor.execute(set_name)
        conn.commit()
        logger.info("A Name field setted.")
    except psycopg2.Error as err:
        conn.rollback()
        logger.error(f"Error setting points name: {err}")


def set_ident_field(conn, cursor, tablename):
    """Add an 'ident' field.

    ident = acq_date:acq_time:latitude:longitude:satellite
    """
    logger.info("Setting Ident field...")
    set_ident = f"""
        UPDATE {tablename}
        SET ident = acq_date \
                    || ':' \
                    || acq_time \
                    || ':' \
                    || to_char(latitude,'999.9999') \
                    || ':' \
                    || to_char(longitude,'999.9999') \
                    || ':' \
                    || satellite
    """
    try:
        cursor.execute(set_ident)
        conn.commit()
        logger.info("A Ident field setted.")
    except psycopg2.Error as err:
        conn.rollback()
        logger.error(f"Error creating ident fields: {err}")


def correct_time_field(conn, cursor, tablename):
    """Correct 'time' field, adding an second zero."""
    logger.info("Correcting Time field...")
    set_ident = f"""
        UPDATE {tablename}
        SET acq_time = left(lpad(acq_time, 4, '0'),2) \
                       || ':' \
                       || right(lpad(acq_time, 4, '0'),2)
    """
    try:
        cursor.execute(set_ident)
        conn.commit()
        logger.info("Time field corrected.")
    except psycopg2.Error as err:
        logger.error(f"Error correcting time fields: {err}")


def set_datetime_field(conn, cursor, tablename):
    """Set 'date_time field ()."""
    logger.info("Setting Date_time field...")
    set_datetime = f"""
        UPDATE {tablename}
        SET date_time = TO_TIMESTAMP(acq_date || ' ' || acq_time,
                                     'YYYY-MM-DD HH24:MI')
    """
    try:
        cursor.execute(set_datetime)
        conn.commit()
        logger.info("Date_time field setted.")
    except psycopg2.Error as err:
        logger.error(f"Error creating timestamp {err}")


def set_marker_field(conn, cursor, tablename, marker):
    """Set 'marker' field."""
    logger.info("Setting Marker field...")
    set_marker = f"""
        UPDATE {tablename}
        SET marker = '{marker}'
    """
    try:
        cursor.execute(set_marker)
        conn.commit()
        logger.info("Marker field setted.")
    except psycopg2.Error as err:
        conn.rollback()
        logger.error(f"Error creating marker: {err}")


def del_duplicates(conn, cursor, tablename):
    """Delete duplicate points."""
    logger.info(f"Deleting duplicates in {tablename}...")
    statements = (
        f"""
            CREATE TABLE {tablename}_tmp AS
                SELECT DISTINCT ON (ident) *
                FROM {tablename}
        """,
        f"""
            DROP TABLE {tablename}
        """,
        f"""
            ALTER TABLE {tablename}_tmp RENAME TO {tablename}
        """
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        cursor.execute(f"SELECT count(*) FROM {tablename}")
        points_count = cursor.fetchone()[0]
        logger.info(f"The duplicates deleted in {tablename}")
        logger.info(f"Final count of rows in {tablename}: {points_count}.")
    except psycopg2.Error as err:
        logger.error(f"Error deleting duplicates: {err}")


def rise_multipoint_cost(conn, cursor, tablename, distance):
    """Rise cost for multipoint."""
    logger.info("Correcting cost for multipoints...")
    temp_tab = f"{tablename}_tmp1"
    clust_tab = f"{tablename}_clst"
    statements = (
        f"""
        DROP TABLE IF EXISTS {temp_tab}
        """,
        f"""
        CREATE TABLE {temp_tab}
            AS SELECT
                ident,
                peat_un_id,
                peat_fire,
                rating,
                critical,
                revision,
                (ST_Transform({tablename}.geog::geometry,
                              3857)::geometry) AS geom
            FROM {tablename}
            WHERE rating > 0
        """,
        f"""
        DROP TABLE IF EXISTS {clust_tab}
        """,
        f"""
        CREATE TABLE {clust_tab} AS
            SELECT
                ST_NumGeometries(gc) as num,
                ST_Buffer(ST_ConvexHull(gc), 50, 'quad_segs=8') AS buffer
            FROM (
                SELECT unnest(ST_ClusterWithin(geom, {distance})) AS gc
                FROM {temp_tab})
                AS result
        """,
        f"""
        DELETE FROM {clust_tab}
            WHERE num < 2
        """,
        f"""
        UPDATE {temp_tab}
        SET
            rating = rating + 1,
            critical = peat_fire*(rating+1),
            revision = 1
        FROM {clust_tab}
        WHERE
            ST_Intersects({temp_tab}.geom, {clust_tab}.buffer)
        """,
        f"""
        UPDATE {tablename}
        SET
            rating = {temp_tab}.rating,
            critical = {temp_tab}.critical
        FROM {temp_tab}
        WHERE
            {temp_tab}.ident = {tablename}.ident
            AND {temp_tab}.revision = 1
        """,
        f"""
        DROP TABLE IF EXISTS {temp_tab}
        """
        # """
        # DROP TABLE IF EXISTS {clust_tab}
        # """
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        logger.info("Cost corrected.")
    except psycopg2.Error as err:
        conn.rollback()
        logger.error(f"Error correcting cost: {err}")


def check_tech_zones(conn, cursor, src_tab, tech_zones):
    """Check if a points in technogen zone."""
    logger.info("Checking tech-zones...")
    sql_stat = f"""
        UPDATE {src_tab}
        SET
            tech = {tech_zones}.name,
            tech_id = {tech_zones}.id
        FROM {tech_zones}
        WHERE
            ST_Intersects({tech_zones}.geog, {src_tab}.geog)
        """
    try:
        cursor.execute(sql_stat)
        conn.commit()
        logger.info("Tech zones checked.")
    except psycopg2.Error as err:
        conn.rollback()
        logger.error(f"'Error intersecting points with tech-zones: {err}")


def check_vip_zones(conn, cursor, src_tab, vip_zones):
    """Check if a points in vip-zone."""
    logger.info("Checking vip-zones...")
    sql_stat = f"""
        UPDATE {src_tab}
        SET
            vip_zone = {vip_zones}.name
        FROM {vip_zones}
        WHERE
            ST_Intersects({vip_zones}.geog, {src_tab}.geog)
        """
    try:
        cursor.execute(sql_stat)
        conn.commit()
        logger.info("Vip zones checked.")
    except psycopg2.Error as err:
        conn.rollback()
        logger.error(f"Error intersecting points with vip-zones: {err}")


def check_oopt_zones(conn, cursor, src_tab, oopt_zones):
    """Check if a points in OOPT-zone."""
    logger.info("Checking oopt-zones...")
    sql_stat = f"""
        UPDATE {src_tab}
        SET
            oopt_id = {oopt_zones}.fid
        FROM {oopt_zones}
        WHERE
            ST_Intersects({oopt_zones}.geog, {src_tab}.geog)
        """
    try:
        cursor.execute(sql_stat)
        conn.commit()
        logger.info("OOPT zones checked.")
    except psycopg2.Error as err:
        conn.rollback()
        logger.error(f"Error intersecting points with oopt-zones: {err}")


def check_oopt_buffers(conn, cursor, src_tab, oopt_buffers):
    """Check if a points in a OOPT-buffers zone."""
    logger.info("Checking oopt buffers...")
    sql_stat = f"""
        UPDATE {src_tab}
        SET
            buffer_id = {oopt_buffers}.fid
        FROM {oopt_buffers}
        WHERE
            ST_Intersects({oopt_buffers}.geog, {src_tab}.geog)
        """
    try:
        cursor.execute(sql_stat)
        conn.commit()
        logger.info("OOPT buffers checked.")
    except psycopg2.Error as err:
        conn.rollback()
        logger.error(f"Error intersecting points with oopt buffers: {err}")


def check_control_zones(conn, cursor, src_tab, control_zones):
    """Check if a points in controled zones."""
    logger.info("Checking control zones...")
    ass_tab = "zones_assign"
    statements = (
        f"""
        DROP TABLE IF EXISTS {ass_tab}
        """,
        f"""
        CREATE TABLE {ass_tab} (
                gid INTEGER,
                zone_id BIGINT,
                category VARCHAR(100)
                )
        """,
        f"""
        INSERT INTO {ass_tab} (gid,
                               zone_id,
                               category)
            SELECT
                {src_tab}.gid AS gid,
                {control_zones}.id AS zone_id,
                {control_zones}.category AS category
            FROM {control_zones},{src_tab}
            WHERE
                ST_Intersects({control_zones}.geog, {src_tab}.geog)
        """,
        f"""
        UPDATE {src_tab}
            SET peat_id = {ass_tab}.zone_id
            FROM {ass_tab}
            WHERE
                {ass_tab}.gid = {src_tab}.gid
                AND category = 'торфяник';
        """,
        f"""
        UPDATE {src_tab}
            SET oopt_id = {ass_tab}.zone_id
            FROM {ass_tab}
            WHERE
                {ass_tab}.gid = {src_tab}.gid
                AND category = 'ООПТ';
        """,
        f"""
        UPDATE {src_tab}
            SET ctrl_id = {ass_tab}.zone_id
            FROM {ass_tab}
            WHERE
                {ass_tab}.gid = {src_tab}.gid
                AND category = 'зона мониторинга';
        """,
        f"""
        UPDATE {src_tab}
            SET safe_id = {ass_tab}.zone_id
            FROM {ass_tab}
            WHERE
                {ass_tab}.gid = {src_tab}.gid
                AND category = 'охранная зона';
        """,
        f"""
        UPDATE {src_tab}
            SET attn_id = {ass_tab}.zone_id
            FROM {ass_tab}
            WHERE
                {ass_tab}.gid = {src_tab}.gid
                AND category = 'зона внимания';
        """,
        f"""
        UPDATE {src_tab}
            SET tech_id = {ass_tab}.zone_id
            FROM {ass_tab}
            WHERE
                {ass_tab}.gid = {src_tab}.gid
                AND category = 'техноген';
        """
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        cursor.execute(f"SELECT count(*) FROM {ass_tab}")
        points_count = cursor.fetchone()[0]
        logger.info("Control zones checked. {points_count} points detected.")
    except psycopg2.Error as err:
        conn.rollback()
        logger.error(f"Error checking control zones: {err}")


def check_monitored_zones(conn, cursor, src_tab, zones_type):
    """Check if a points in monitored zones."""
    monitored_zones = zones_type + '_zones'
    logger.info(f"Checking {monitored_zones}...")
    sql_stat = f"""
        UPDATE {src_tab}
            SET
                {zones_type}_id = {monitored_zones}.id,
                l_code = {monitored_zones}.l_code
            FROM {monitored_zones}
            WHERE
                ST_Intersects({monitored_zones}.geog, {src_tab}.geog);
        """
    count_stat = f"""
        SELECT
            count(*)
        FROM
            {src_tab}
        WHERE
            {zones_type}_id IS NOT NULL
        """
    try:
        cursor.execute(sql_stat)
        conn.commit()
        cursor.execute(count_stat)
        points_count = cursor.fetchone()[0]
        logger.info(f"Zones checked. {points_count} points detected.")
    except psycopg2.Error as err:
        conn.rollback()
        logger.error(f"Error checking zones: {err}")


def check_control_buffers(conn, cursor, src_tab, control_zones, buffers):
    """Check if a points in a control zones buffers."""
    logger.info("Checking control zones buffers...")
    ass_tab = "buffers_assign"
    statements = (
        f"""
        DROP TABLE IF EXISTS {ass_tab}
        """,
        f"""
        CREATE TABLE {ass_tab} (
                gid INTEGER,
                zone_id BIGINT,
                category VARCHAR(100),
                distance INTEGER
                )
        """,
        f"""
        INSERT INTO {ass_tab} (gid,
                               zone_id,
                               category)
            SELECT
                {src_tab}.gid AS gid,
                {buffers}.id AS zone_id,
                {buffers}.category AS category
            FROM {buffers},{src_tab}
            WHERE
                ST_Intersects({buffers}.geog, {src_tab}.geog)
        """,
        f"""
        UPDATE {ass_tab}
            SET distance = ST_Distance({src_tab}.geog, {control_zones}.geog)
            FROM {src_tab}, {control_zones}
            WHERE {src_tab}.gid = {ass_tab}.gid
                    AND {ass_tab}.zone_id = {control_zones}.id
        """,
        f"""
        UPDATE {src_tab}
            SET
                peat_buf_id = {ass_tab}.zone_id,
                distance = {ass_tab}.distance
            FROM {ass_tab}
            WHERE
                {ass_tab}.gid = {src_tab}.gid
                AND category = 'торфяник';
        """,
        f"""
        UPDATE {src_tab}
            SET
                oopt_buf_id = {ass_tab}.zone_id,
                distance = {ass_tab}.distance
            FROM {ass_tab}
            WHERE
                {ass_tab}.gid = {src_tab}.gid
                AND category = 'ООПТ';
        """,
        f"""
        UPDATE {src_tab}
            SET
                ctrl_buf_id = {ass_tab}.zone_id,
                distance = {ass_tab}.distance
            FROM {ass_tab}
            WHERE
                {ass_tab}.gid = {src_tab}.gid
                AND category = 'зона мониторинга';
        """,
        f"""
        UPDATE {src_tab}
            SET
                safe_buf_id = {ass_tab}.zone_id,
                distance = {ass_tab}.distance
            FROM {ass_tab}
            WHERE
                {ass_tab}.gid = {src_tab}.gid
                AND category = 'охранная зона';
        """,
        f"""
        UPDATE {src_tab}
            SET
                attn_buf_id = {ass_tab}.zone_id,
                distance = {ass_tab}.distance
            FROM {ass_tab}
            WHERE
                {ass_tab}.gid = {src_tab}.gid
                AND category = 'зона внимания';
        """
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        cursor.execute(f"SELECT count(*) FROM {ass_tab}")
        points_count = cursor.fetchone()[0]
        logger.info("Control buffers checked. {points_count} points detected.")
    except psycopg2.Error as err:
        conn.rollback()
        logger.error(f"Error checking control buffers: {err}")


def check_monitored_buffers(conn, cursor, src_tab, zones_type):
    """Check if a points in a monitored zones buffers."""
    monitored_zones = zones_type + '_zones'
    buffers = zones_type + '_zones_buf'
    logger.info(f"Checking {monitored_zones} buffers...")
    statements = (
        f"""
        UPDATE {src_tab}
            SET
                {zones_type}_id = {buffers}.id,
                l_code = {buffers}.l_code,
                {zones_type}_dist = 1000000
            FROM {buffers}
            WHERE
                ST_Intersects({buffers}.geog, {src_tab}.geog);
        """,
        f"""
        UPDATE {src_tab}
            SET
                {zones_type}_dist = ST_Distance(src.geog, zones.geog)
            FROM
                {src_tab} AS src JOIN {monitored_zones} AS zones
                ON src.{zones_type}_id = zones.id 
                   AND src.{zones_type}_dist = 1000000
            WHERE
                {src_tab}.gid = src.gid 
        """
    )
    count_stat = f"""
        SELECT
            count(*)
        FROM
            {src_tab}
        WHERE
            {zones_type}_dist IS NOT NULL
        """
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        cursor.execute(count_stat)
        points_count = cursor.fetchone()[0]
        logger.info(f"Buff checked. {points_count} points detected.")
    except psycopg2.Error as err:
        conn.rollback()
        logger.error(f"Error checking buffers: {err}")


def copy_to_common_table(conn, cursor, today_tab, year_tab):
    """Copy temporary common table to year table."""
    logger.info("Copying data into common table...")
    ins_string = f"""
        INSERT INTO {year_tab} (name,
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
                                scan,
                                track,
                                version,
                                frp,
                                region,
                                rating,
                                critical,
                                revision,
                                peat_un_id,
                                peat_district,
                                peat_region,
                                peat_area,
                                peat_class,
                                peat_fire,
                                ident,
                                date_time,
                                geog,
                                marker,
                                tech,
                                vip_zone,
                                oopt_id,
                                country,
                                oopt,
                                peat_id,
                                ctrl_id,
                                safe_id,
                                attn_id,
                                tech_id,
                                peat_buf_id,
                                oopt_buf_id,
                                ctrl_buf_id,
                                safe_buf_id,
                                attn_buf_id,
                                distance,
                                zone_id,
                                l_code,
                                peat_dist,
                                attn_dist,
                                oopt_dist)
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
                scan,
                track,
                version,
                frp,
                region,
                rating,
                critical,
                revision,
                peat_un_id,
                peat_district,
                peat_region,
                peat_area,
                peat_class,
                peat_fire,
                ident,
                date_time,
                geog,
                marker,
                tech,
                vip_zone,
                oopt_id,
                country,
                oopt,
                peat_id,
                ctrl_id,
                safe_id,
                attn_id,
                tech_id,
                peat_buf_id,
                oopt_buf_id,
                ctrl_buf_id,
                safe_buf_id,
                attn_buf_id,
                distance,
                zone_id,
                l_code,
                peat_dist,
                attn_dist,
                oopt_dist
            FROM {today_tab}
            WHERE NOT EXISTS(
                SELECT ident FROM {year_tab}
                WHERE
                    {today_tab}.ident = {year_tab}.ident)
    """
    try:
        cursor.execute(ins_string)
        conn.commit()
        logger.info(f"Data from {today_tab} added to common table {year_tab}")
    except psycopg2.Error as err:
        logger.error(f"Error addin points to common table: {err}")


def drop_today_table(conn, cursor, common_tab):
    """Drop common today table."""
    logger.info("Dropping today table...")
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {common_tab}")
        conn.commit()
        logger.info("Today table dropped.")
    except psycopg2.Error as err:
        conn.rollback()
        logger.error(f"Error dropping today table: {err}")


def get_and_merge_points_job():
    """Get and merge points main job."""
    currtime = time.localtime()
    date = time.strftime("%Y-%m-%d", currtime)

    logger.info("------------------------------------------")
    logger.info("Process [get_and_merge_points.py] started.")

    [year_tab,
     common_tab,
     tech_zones,
     vip_zones,
     oopt_zones,
     oopt_buffers,
     zones_tabs,
     zones_types] = get_config("tables", ["year_tab",
                                         "common_tab",
                                         "tech_zones",
                                         "vip_zones",
                                         "oopt_zones",
                                         "oopt_buffers",
                                         "zones_tabs",
                                         "zones_types"])
    [data_root, firms_folder] = get_config("path", ["data_root",
                                                    "firms_folder"])
    [clst_dist] = get_config("clusters", ["cluster_dist"])
    [num_of_src] = get_config("sources", ["num_of_src"])
    [pointsets] = get_config("sources", ["src"])
    [url, chat_id] = get_config("telegramm", ["url", "log_chat_id"])
    firms_path = get_path(data_root, firms_folder)

    loaded_set = []

    conn, cursor = get_cursor()

    for pointset in pointsets:
        errcode = GetPoints_with_retries(pointset, firms_path, date)
        if errcode == 0:
            drop_today_tables(conn, cursor, pointset)
            count = upload_points_to_db(cursor, firms_path, pointset, date)
            if count > 0:
                add_geog_field(conn, cursor, pointset)
                make_tables_for_mon_area(conn, cursor, pointset)
                loaded_set.append(pointset)
            else:
                msg = f"Zero-rows file: {pointset}"
                send_to_telegram(url, chat_id, msg)

    loaded = make_common_table(conn, cursor, common_tab, loaded_set)

    if loaded < int(num_of_src):
        msg = f"Загружены данные из {loaded} таблиц"
        # send_to_telegram(url, chat_id, msg)

    correct_time_field(conn, cursor, common_tab)
    set_datetime_field(conn, cursor, common_tab)
    marker = ""
    set_marker_field(conn, cursor, common_tab, marker)
    set_ident_field(conn, cursor, common_tab)
    del_duplicates(conn, cursor, common_tab)
    cost_point_in_buffers(conn, cursor, common_tab)
    set_name_field(conn, cursor, common_tab)
    rise_multipoint_cost(conn, cursor, common_tab, clst_dist)
    check_tech_zones(conn, cursor, common_tab, tech_zones)
    # check_vip_zones(conn, cursor, common_tab, vip_zones)
    # check_oopt_zones(conn, cursor, common_tab, oopt_zones)
    # check_oopt_buffers(conn, cursor, common_tab, oopt_buffers)
    # check_control_zones(conn, cursor, common_tab, oopt_zones)
    # check_control_buffers(conn, cursor, common_tab,
    #                      oopt_zones, oopt_buffers)
    for zones_type in zones_types:
        zones_tab = zones_type + '_zones'
        zones_buf_tab = zones_tab + '_buf'
        check_monitored_zones(conn, cursor, common_tab, zones_type)
        check_monitored_buffers(conn, cursor, common_tab,
                                zones_type)
    copy_to_common_table(conn, cursor, common_tab, year_tab)
    for pointset in pointsets:
        drop_temp_tables(conn, cursor, pointset)
    drop_today_table(conn, cursor, common_tab)

    close_conn(conn, cursor)
    logger.info("Process [get_and_merge_points.py] stopped.")


if __name__ == "__main__":
    get_and_merge_points_job()
