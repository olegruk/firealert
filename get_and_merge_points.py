﻿"""
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
from falogging import (
    log,
    start_logging,
    stop_logging)
from faservice import (
    get_config,
    get_db_config,
    get_cursor,
    close_conn,
    get_path,
    send_to_telegram)


def MakeTodayDir(DateStr, aDir):
    """Create a subdir, based on current date."""
    log(f"Creating today dir {aDir}...")
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


def get_session(url):
    """Get session for download firepoints."""
    log(f"Requesting session {url}...")
    s = requests.session()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": url,
        "Pragma": "no-cache",
        "Cache-Control": "no-cache"}
    r = s.get(url, headers=headers)
    # r = s.get(url, headers=headers, verify=False)
    log("Session created.")
    return r


def read_csv_from_site(url, sourcepath):
    """Read file from site and save to csv."""
    try:
        filereq = requests.get(url, stream=True)
        filereq.raise_for_status()
    except HTTPError as http_err:
        log(f"HTTP error occurred: {http_err}")
        errcode = 2
    except Exception as err:
        log(f"Other error occurred: {err}")
        errcode = 3
    else:
        log(f"Get receive status code {filereq.status_code}")
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
        log(f"Connection error occurred: {conn_err}")
        errcode = 1
    except HTTPError as http_err:
        log(f"HTTP error occurred: {http_err}")
        errcode = 2
    except RequestException as req_err:
        log(f"Request error occurred: {req_err}")
        errcode = 3
    except Exception as err:
        log(f"Other error occurred: {err}")
        errcode = 4
    else:
        log(f"Get receive status code {filereq.status_code}")
        # filereq = requests.get(url, stream=True, verify=False)
        with open(sourcepath, "wb") as receive:
            shutil.copyfileobj(filereq.raw, receive)
            del filereq
        errcode = 0
    return errcode


def GetPoints(pointset, dst_folder, aDate):
    """Get csv-file from NASA site."""
    log(f"Getting points for {pointset}...")
    [period] = get_config("NASA", ["load_period"])
    [src_url] = get_config("NASA", [f"{pointset}_src_{period}"])
    dst_file = f"{pointset}_{aDate}.csv"
    dst_file = os.path.join(dst_folder, dst_file)
    errcode = read_csv_from_site(src_url, dst_file)
    if errcode == 0:
        log(f"Download complete: {dst_file}")
    else:
        if os.path.exists(dst_file):
            try:
                os.remove(dst_file)
            except OSError as err:
                log(f"Unable to remove file: {dst_file}.\nError {err}")
        log(f"Not downloaded: {dst_file}.")
    return errcode


def GetPoints_with_retries(pointset, dst_folder, aDate):
    """Get csv-file from NASA site with retries."""
    log(f"Getting points for {pointset}...")
    [period] = get_config("NASA", ["load_period"])
    [src_url] = get_config("NASA", [f"{pointset}_src_{period}"])
    [src_url2] = get_config("NASA", [f"{pointset}_src2_{period}"])
    dst_file = f"{pointset}_{aDate}.csv"
    dst_file = os.path.join(dst_folder, dst_file)
    errcode = read_csv_from_site_with_retries(src_url, dst_file)
    if errcode == 0:
        log(f"Download complete: {dst_file}.")
    else:
        errcode = read_csv_from_site_with_retries(src_url2, dst_file)
        if errcode == 0:
            log(f"Download complete: {dst_file}.")
        else:
            if os.path.exists(dst_file):
                try:
                    os.remove(dst_file)
                except OSError as err:
                    log(f"Unable to remove file: {dst_file}.\nError {err}")
            log(f"Not downloaded: {dst_file}.")
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
    log("Writing points...")
    try:
        dataframe.to_sql(tablename,
                         engine,
                         index=False,
                         if_exists=u"append",
                         chunksize=1000)
        log("Done inserted source into postgres")
    except IOError as err:
        log(f"Error in inserting data into db: {err}")


def upload_points_to_db(cursor, src_folder, pointset, aDate):
    """Upload points from csv-file into database."""
    log(f"Upload points {pointset} into postgres...")
    engine = create_engine("postgresql+psycopg2://", creator=getconn)

    src_file = f"{pointset}_{aDate}.csv"
    src_file = os.path.join(src_folder, src_file)
    dst_table = f"{pointset}_today"
    try:
        csv_src = pandas.read_csv(src_file)
        write_to_db(engine, dst_table, csv_src)
        cursor.execute(f"SELECT count(*) FROM {dst_table}")
        points_count = cursor.fetchone()[0]
        log(f"{points_count} rows added to db from {src_file}")
    except IOError as err:
        log("Error download and add data {err}")
        points_count = 0
    return points_count


def drop_today_tables(conn, cursor, pointset):
    """Drop temporary today tables."""
    log(f"Dropping today tables for {pointset}...")
    today_tab = f"{pointset}_today"
    sql_stat = f"DROP TABLE IF EXISTS {today_tab}"
    try:
        cursor.execute(sql_stat)
        conn.commit()
        log("Tables dropped")
    except psycopg2.Error as err:
        log(f"Error dropping table: {err}")


def drop_temp_tables(conn, cursor, pointset):
    """Drop temporary tables."""
    log(f"Dropping temp tables for {pointset}...")
    today_tab = f"{pointset}_today"
    today_tab_ru = f"{pointset}_today_ru"
    sql_stat_1 = f"DROP TABLE IF EXISTS {today_tab}"
    sql_stat_2 = f"DROP TABLE IF EXISTS {today_tab_ru}"
    try:
        cursor.execute(sql_stat_1)
        cursor.execute(sql_stat_2)
        conn.commit()
        log("Temp tables dropped.")
    except psycopg2.Error as err:
        log(f"Error dropping temp tables: {err}")


def add_geog_field(conn, cursor, pointset):
    """Add geog field to today-table."""
    log(f"Adding geog field for {pointset}...")
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
        """
        CREATE INDEX {src_tab}_idx ON {src_tab} USING GIST (geog)
        """
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
        conn.commit()
        log(f"Geometry added to {src_tab}.")
    except psycopg2.Error as err:
        log(f"Error adding geometry: {err}")


def make_tables_for_Russia(conn, cursor, pointset):
    """Make firepoints subset for Russia."""
    log(f"Making table for Russia for {pointset}...")
    src_tab = f"{pointset}_today"
    dst_tab = f"{pointset}_today_ru"
    [outline] = get_config("regions", ["reg_russia"])
    statements = (
        f"""
        DROP TABLE IF EXISTS {dst_tab}
        """,
        f"""
        CREATE TABLE {dst_tab} AS
            SELECT {src_tab}.*, {outline}.region
            FROM {src_tab}, {outline}
            WHERE
                ST_Intersects({outline}.geog, {src_tab}.geog)
        """
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log(f"The table created: {dst_tab}.")
    except psycopg2.Error as err:
        log(f"Error intersecting points with region: {err}")


def make_common_table(conn, cursor, dst_tab, pointsets):
    """Merge points from source tables into common table."""
    log("Making common table...")
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
                tech VARCHAR(254),
                vip_zone VARCHAR(254),
                oopt VARCHAR(254),
                oopt_id INTEGER,
                oopt_buf_id INTEGER
        )
        """
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log(f"The table created: {dst_tab}.")
    except psycopg2.Error as err:
        log(f"Error creating table: {err}")

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
                    {src_tab}.geog
                FROM {src_tab}
        """

        try:
            if pointset in ["as_modis", "eu_modis"]:
                cursor.execute(ins_from_modis)
            elif pointset in ["as_viirs", "eu_viirs", "as_vnoaa", "eu_vnoaa"]:
                cursor.execute(ins_from_viirs)
            conn.commit()
            log(f"The data added: {src_tab}")
        except psycopg2.Error as err:
            log(f"Error adding data: {err}")

        loaded = loaded + 1
    return loaded


def cost_point_in_buffers(conn, cursor, tablename):
    """Cost points located in peat buffers."""
    log("Costing points in buffers...")
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
                    peat_id = {peat_db}.unique_id,
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
            log(f"The rating {rate} setted to {peat_db}.")
        set_zero_rating = f"""
            UPDATE {tablename}
            SET
                rating = 0,
                critical = 0
            WHERE
                critical IS NULL
        """
        cursor.execute(set_zero_rating)
        log("Zero rating setted.")
        conn.commit()
    except psycopg2.Error as err:
        log(f"Error costing points: {err}")


def set_name_field(conn, cursor, tablename):
    """Add a 'name' field (name = acq_date : gid : critical)."""
    log("Setting 'name' field...")

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
        log("A Name field setted.")
    except psycopg2.Error as err:
        log(f"Error setting points name: {err}")


def set_ident_field(conn, cursor, tablename):
    """Add an 'ident' field.

    ident = acq_date:acq_time:latitude:longitude:satellite
    """
    log("Setting Ident field...")
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
        log("A Ident field setted.")
    except psycopg2.Error as err:
        log(f"Error creating ident fields: {err}")


def correct_time_field(conn, cursor, tablename):
    """Correct 'time' field, adding an second zero."""
    log("Correcting Time field...")
    set_ident = f"""
        UPDATE {tablename}
        SET acq_time = left(lpad(acq_time, 4, '0'),2) \
                       || ':' \
                       || right(lpad(acq_time, 4, '0'),2)
    """
    try:
        cursor.execute(set_ident)
        conn.commit()
        log("Time field corrected.")
    except psycopg2.Error as err:
        log(f"Error correcting time fields: {err}")


def set_datetime_field(conn, cursor, tablename):
    """Set 'date_time field ()."""
    log("Setting Date_time field...")
    set_datetime = f"""
        UPDATE {tablename}
        SET date_time = TO_TIMESTAMP(acq_date || ' ' || acq_time,
                                     'YYYY-MM-DD HH24:MI')
    """
    try:
        cursor.execute(set_datetime)
        conn.commit()
        log("Date_time field setted.")
    except psycopg2.Error as err:
        log(f"Error creating timestamp {err}")


def set_marker_field(conn, cursor, tablename, marker):
    """Set 'marker' field."""
    log("Setting Marker field...")
    set_marker = f"""
        UPDATE {tablename}
        SET marker = '{marker}'
    """
    try:
        cursor.execute(set_marker)
        conn.commit()
        log("Marker field setted.")
    except psycopg2.Error as err:
        log(f"Error creating marker: {err}")


def del_duplicates(conn, cursor, tablename):
    """Delete duplicate points."""
    log(f"Deleting duplicates in {tablename}...")
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
        log(f"The duplicates deleted in {tablename}")
    except psycopg2.Error as err:
        log(f"Error deleting duplicates: {err}")


def rise_multipoint_cost(conn, cursor, tablename, distance):
    """Rise cost for multipoint."""
    log("Correcting cost for multipoints...")
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
                peat_id,
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
        log("Cost corrected.")
    except psycopg2.Error as err:
        log(f"Error correcting cost: {err}")


def check_tech_zones(conn, cursor, src_tab, tech_zones):
    """Check if a points in technogen zone."""
    log("Checking tech-zones...")
    sql_stat = f"""
        UPDATE {src_tab}
        SET
            tech = {tech_zones}.name
        FROM {tech_zones}
        WHERE
            ST_Intersects({tech_zones}.geog, {src_tab}.geog)
        """
    try:
        cursor.execute(sql_stat)
        conn.commit()
        log("Tech zones checked.")
    except psycopg2.Error as err:
        log(f"'Error intersecting points with tech-zones: {err}")


def check_vip_zones(conn, cursor, src_tab, vip_zones):
    """Check if a points in vip-zone."""
    log("Checking vip-zones...")
    sql_stat = f"""
        UPDATE {src_tab}
        SET
            vip_zone = {vip_zones}.name
        FROM {vip_zones}
        WHERE
            ST_Intersects(%(o)s.geog, {src_tab}.geog)
        """
    try:
        cursor.execute(sql_stat)
        conn.commit()
        log("Vip zones checked.")
    except psycopg2.Error as err:
        log(f"Error intersecting points with vip-zones: {err}")


def check_oopt_zones(conn, cursor, src_tab, oopt_zones):
    """Check if a points in OOPT-zone."""
    log("Checking oopt-zones...")
    sql_stat = f"""
        UPDATE {src_tab}
        SET
            oopt_id = {oopt_zones}.fid
        FROM {oopt_zones}
        WHERE
            ST_Intersects(%(o)s.geog, {src_tab}.geog)
        """
    try:
        cursor.execute(sql_stat)
        conn.commit()
        log("OOPT zones checked.")
    except psycopg2.Error as err:
        log(f"Error intersecting points with oopt-zones: {err}")


def check_oopt_buffers(conn, cursor, src_tab, oopt_buffers):
    """Check if a points in a OOPT-buffers zone."""
    log("Checking oopt buffers...")
    sql_stat = f"""
        UPDATE {src_tab}
        SET
            oopt_buf_id = %(o)s.fid
        FROM {oopt_buffers}
        WHERE
            ST_Intersects({oopt_buffers}.geog, {src_tab}.geog)
        """
    try:
        cursor.execute(sql_stat)
        conn.commit()
        log("OOPT buffers checked.")
    except psycopg2.Error as err:
        log(f"Error intersecting points with oopt buffers: {err}")


def copy_to_common_table(conn, cursor, today_tab, year_tab):
    """Copy temporary common table to year table."""
    log("Copying data into common table...")
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
                                tech,
                                vip_zone,
                                oopt_id,
                                oopt_buf_id)
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
                tech,
                vip_zone,
                oopt_id,
                oopt_buf_id
            FROM {today_tab}
            WHERE NOT EXISTS(
                SELECT ident FROM {year_tab}
                WHERE
                    {today_tab}.ident = {year_tab}.ident)
    """
    try:
        cursor.execute(ins_string)
        conn.commit()
        log(f"Data from {today_tab} added to common table {year_tab}")
    except psycopg2.Error as err:
        log(f"Error addin points to common table: {err}")


def drop_today_table(conn, cursor, common_tab):
    """Drop common today table."""
    log("Dropping today table...")
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {common_tab}")
        conn.commit()
        log("Today table dropped.")
    except psycopg2.Error as err:
        log(f"Error dropping today table: {err}")


def get_and_merge_points_job():
    """Get and merge points main job."""
    currtime = time.localtime()
    date = time.strftime("%Y-%m-%d", currtime)

    start_logging("get_and_merge_points.py")

    [year_tab,
     common_tab,
     tech_zones,
     vip_zones,
     oopt_zones,
     oopt_buffers] = get_config("tables", ["year_tab",
                                           "common_tab",
                                           "tech_zones",
                                           "vip_zones",
                                           "oopt_zones",
                                           "oopt_buffers"])
    [data_root, firms_folder] = get_config("path", ["data_root",
                                                    "firms_folder"])
    [clst_dist] = get_config("clusters", ["cluster_dist"])
    [num_of_src] = get_config("sources", ["num_of_src"])
    [pointsets] = get_config("sources", ["src"])
    [url, chat_id] = get_config("telegramm", ["url", "tst_chat_id"])
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
                make_tables_for_Russia(conn, cursor, pointset)
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
    check_vip_zones(conn, cursor, common_tab, vip_zones)
    check_oopt_zones(conn, cursor, common_tab, oopt_zones)
    check_oopt_buffers(conn, cursor, common_tab, oopt_buffers)
    copy_to_common_table(conn, cursor, common_tab, year_tab)
    for pointset in pointsets:
        drop_temp_tables(conn, cursor, pointset)
    drop_today_table(conn, cursor, common_tab)

    close_conn(conn, cursor)
    stop_logging("get_and_merge_points.py")


if __name__ == "__main__":
    get_and_merge_points_job()
