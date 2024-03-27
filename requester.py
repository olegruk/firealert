"""
Module of firealert robot.

Contains a lot of db-request functions.

Created:     13.05.2020

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
from faservice import (
    get_config,
    get_cursor,
    close_conn,
    write_to_kml)
from mylogger import get_logger

logger = get_logger()


def make_reqst_table(conn, cursor, src_tab, crit_or_peat, limit,
                     from_time, period, reg_list, whom, is_incremental):
    """Create a table for upload to subscribers."""
    logger.info(f"Creating table for subs_id: {whom}...")
    subs_tab = f"for_s{str(whom)}"
    period = f"{period} hours"

    statements_regional_yesterday = (
        f"""
        DROP TABLE IF EXISTS {subs_tab}
        """,
        f"""
        CREATE TABLE {subs_tab} (
                name VARCHAR(30),
                description VARCHAR(500),
                acq_date VARCHAR(10),
                acq_time VARCHAR(5),
                latitude NUMERIC,
                longitude NUMERIC,
                sat_sensor VARCHAR(5),
                region  VARCHAR(100),
                peat_district VARCHAR(254),
                peat_un_id VARCHAR(256),
                peat_class SMALLINT,
                peat_fire SMALLINT,
                critical SMALLINT,
                geog GEOGRAPHY(POINT, 4326)
                )
        """,
        f"""
        INSERT INTO {subs_tab} (name,
                                acq_date,
                                acq_time,
                                latitude,
                                longitude,
                                sat_sensor,
                                region,
                                critical,
                                peat_un_id,
                                peat_district,
                                peat_class,
                                peat_fire,
                                geog)
            SELECT
                {src_tab}.name,
                {src_tab}.acq_date,
                {src_tab}.acq_time,
                {src_tab}.latitude,
                {src_tab}.longitude,
                {src_tab}.satellite,
                {src_tab}.region,
                {src_tab}.critical,
                {src_tab}.peat_un_id,
                {src_tab}.peat_district,
                {src_tab}.peat_class,
                {src_tab}.peat_fire,
                {src_tab}.geog
            FROM {src_tab}
            WHERE
                date_time > TIMESTAMP '{from_time}' - INTERVAL '{period}'
                AND "date_time" <= TIMESTAMP '{from_time}'
                AND {crit_or_peat} >= {limit}
                AND region IN ({reg_list})
            ORDER BY
                {src_tab}.peat_un_id
        """,
        f"""
        UPDATE {subs_tab}
            SET
                sat_sensor = 'VIIRS'
            WHERE
                sat_sensor = 'N'
        """,
        f"""
        UPDATE {subs_tab}
            SET
                sat_sensor = 'MODIS'
            WHERE
                sat_sensor <> 'VIIRS'
        """,
        f"""
        UPDATE {subs_tab}
            SET
                description =
                    'Дата: ' || acq_date || '\n' ||
                    'Время: ' || acq_time || '\n' ||
                    'Сенсор: ' || sat_sensor || '\n' ||
                    'Регион: ' || region || '\n' ||
                    'Район: ' || peat_district || '\n' ||
                    'Торфяник (ID): ' || peat_un_id || '\n' ||
                    'Класс осушки: ' || peat_class || '\n' ||
                    'Горимость торфяника: ' || peat_fire || '\n' ||
                    'Критичность точки: ' || critical
        """,
        f"""
        UPDATE {subs_tab}
            SET
                description =
                    'Дата: ' || acq_date || '\n' ||
                    'Время: ' || acq_time || '\n' ||
                    'Сенсор: ' || sat_sensor || '\n' ||
                    'Регион: ' || region
            WHERE
                peat_un_id IS NULL
        """
    )

    statements_allrussia_yesterday = (
        f"""
        DROP TABLE IF EXISTS {subs_tab}
        """,
        f"""
        CREATE TABLE {subs_tab} (
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
        """,
        f"""
        INSERT INTO {subs_tab} (acq_date,
                                acq_time,
                                latitude,
                                longitude,
                                sat_sensor,
                                region,
                                geog)
            SELECT
                {src_tab}.acq_date,
                {src_tab}.acq_time,
                {src_tab}.latitude,
                {src_tab}.longitude,
                {src_tab}.satellite,
                {src_tab}.region,
                {src_tab}.geog
            FROM
                {src_tab}
            WHERE
                country = 'Россия'
                AND date_time > TIMESTAMP '{from_time}' - INTERVAL '{period}'
                AND "date_time" <= TIMESTAMP '{from_time}'
        """,
        f"""
        UPDATE {subs_tab}
            SET
                sat_sensor = 'VIIRS'
            WHERE
                sat_sensor = 'N'
        """,
        f"""
        UPDATE {subs_tab}
            SET
                sat_sensor = 'MODIS'
            WHERE
                sat_sensor <> 'VIIRS'
        """,
        f"""
        UPDATE {subs_tab}
            SET
                name = ''
        """,
        f"""
        UPDATE {subs_tab}
            SET
                description =
                    'Дата: ' || acq_date || '\n' ||
                    'Время: ' || acq_time || '\n' ||
                    'Сенсор: ' || sat_sensor || '\n' ||
                    'Регион: ' || region
        """
    )

    if reg_list == "'Россия'":
        statements = statements_allrussia_yesterday
    else:
        statements = statements_regional_yesterday

    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        logger.info(f"The table created for subs_id: {whom}")
    except IOError as err:
        logger.error(f"Error creating subscribers tables: {err}")
    cursor.execute(f"SELECT count(*) FROM {subs_tab}")
    return cursor.fetchone()[0]


def make_reqst_for_circle(conn, cursor, src_tab, crit_or_peat,
                          limit, from_time, period, circle, whom):
    """Make a table with firepoints in radius for uploads."""
    logger.info(f"Creating table of points in circle for subs_id: {whom}...")
    subs_tab = f"for_s{str(whom)}"
    # period = '%s hours' %period
    cent_x = circle[0]
    cent_y = circle[1]
    radius = circle[2]
    statements = (
        f"""
        DROP TABLE IF EXISTS {subs_tab}
        """,
        f"""
        CREATE TABLE {subs_tab} (
                name VARCHAR(30),
                description VARCHAR(500),
                acq_date VARCHAR(10),
                acq_time VARCHAR(5),
                latitude NUMERIC,
                longitude NUMERIC,
                sat_sensor VARCHAR(5),
                region  VARCHAR(100),
                peat_district VARCHAR(254),
                peat_un_id VARCHAR(256),
                peat_class SMALLINT,
                peat_fire SMALLINT,
                critical SMALLINT,
                geog GEOGRAPHY(POINT, 4326)
                )
        """,
        f"""
        INSERT INTO {subs_tab} (name,
                        acq_date,
                        acq_time,
                        latitude,
                        longitude,
                        sat_sensor,
                        region,
                        critical,
                        peat_un_id,
                        peat_district,
                        peat_class,
                        peat_fire,
                        geog)
            SELECT
                {src_tab}.name,
                {src_tab}.acq_date,
                {src_tab}.acq_time,
                {src_tab}.latitude,
                {src_tab}.longitude,
                {src_tab}.satellite,
                {src_tab}.region,
                {src_tab}.critical,
                {src_tab}.peat_un_id,
                {src_tab}.peat_district,
                {src_tab}.peat_class,
                {src_tab}.peat_fire,
                {src_tab}.geog
            FROM {src_tab}
            WHERE
                date_time > TIMESTAMP '{from_time}' - INTERVAL '{period}'
                AND "date_time" <= TIMESTAMP '{from_time}'
                AND {crit_or_peat} >= {limit}
                AND ST_DWithin({src_tab}.geog,
                            ST_GeogFromText(
                                'SRID=4326;POINT({cent_x} {cent_y})'),
                            {radius})
        """,
        f"""
        UPDATE {subs_tab}
            SET
                sat_sensor = 'VIIRS'
            WHERE
                sat_sensor = 'N'
        """,
        f"""
        UPDATE {subs_tab}
            SET
                sat_sensor = 'MODIS'
            WHERE
                sat_sensor <> 'VIIRS'
        """,
        f"""
        UPDATE {subs_tab}
            SET
                description =
                    'Дата: ' || acq_date || '\n' ||
                    'Время: ' || acq_time || '\n' ||
                    'Сенсор: ' || sat_sensor || '\n' ||
                    'Регион: ' || region || '\n' ||
                    'Район: ' || peat_district || '\n' ||
                    'Торфяник (ID): ' || peat_un_id || '\n' ||
                    'Класс осушки: ' || peat_class || '\n' ||
                    'Горимость торфяника: ' || peat_fire || '\n' ||
                    'Критичность точки: ' || critical
        """,
        f"""
        UPDATE {subs_tab}
            SET
            description =
                'Дата: ' || acq_date || '\n' ||
                'Время: ' || acq_time || '\n' ||
                'Сенсор: ' || sat_sensor || '\n' ||
                'Регион: ' || region
            WHERE
                peat_un_id IS NULL
        """
    )

    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        logger.info(f"The table created: subs_id: {whom}")
    except IOError as err:
        logger.error(f"Error creating subscribers tables: {err}")
    cursor.execute(f"SELECT count(*) FROM {subs_tab}")
    return cursor.fetchone()[0]


def drop_whom_table(conn, cursor, whom):
    """Drop temporary whom table."""
    subs_tab = f"for_s{str(whom)}"
    logger.info(f"Dropping table {subs_tab}")
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {subs_tab}")
        conn.commit()
        logger.info("Table dropped.")
    except IOError as err:
        logger.error(f"Error dropping table: {err}")


def make_file_name(period, date, whom, result_dir, iter):
    """Make name of file generated for subscriber."""
    if iter == 0:
        suff = ""
    else:
        suff = f"_inc{str(iter)}"
    if period == "24 hours":
        dst_file_name = f"{date}_{whom}{suff}.kml"
    else:
        period_mod = period
        period_mod = period_mod.replace(' ', '_')
        dst_file_name = f"{date}_{whom}_{period_mod}{suff}.kml"
    dst_file = os.path.join(result_dir, dst_file_name)
    if os.path.isfile(dst_file):
        iter = iter + 1
        dst_file_name = make_file_name(period, date, whom, result_dir, iter)
    return dst_file_name


def request_data(whom, lim_for, limit, from_time, period, regions, result_dir):
    """Request data for subscriber and write it to kml-file."""
    currtime = time.localtime()
    date = time.strftime("%Y-%m-%d", currtime)

    [year_tab] = get_config("tables", ["year_tab"])
    conn, cursor = get_cursor()

    num_points = make_reqst_table(conn, cursor, year_tab, lim_for, limit,
                                  from_time, period, regions, whom, False)
    dst_file_name = make_file_name(period, date, whom, result_dir, 0)
    dst_file = os.path.join(result_dir, dst_file_name)
    write_to_kml(dst_file, whom)
    drop_whom_table(conn, cursor, whom)

    close_conn(conn, cursor)

    return dst_file, num_points


def request_for_circle(whom, lim_for, limit, from_time,
                       period, circle, result_dir):
    """Request firepoints into circle and write it to kml-file."""
    currtime = time.localtime()
    date = time.strftime("%Y-%m-%d", currtime)

    [year_tab] = get_config("tables", ["year_tab"])
    conn, cursor = get_cursor()

    num_points = make_reqst_for_circle(conn, cursor, year_tab, lim_for,
                                       limit, from_time, period, circle, whom)
    dst_file_name = make_file_name(period, date, whom, result_dir, 0)
    dst_file = os.path.join(result_dir, dst_file_name)
    write_to_kml(dst_file, whom)
    drop_whom_table(conn, cursor, whom)

    close_conn(conn, cursor)

    return dst_file, num_points


def new_alerts(period, cur_date):
    """Check a new alerts for period."""
    logger.info("Adding alerts...")
    # extract params from config
    [alert_tab, clust_view] = get_config("peats_stat",
                                         ["alert_tab", "cluster_view"])
    conn, cursor = get_cursor()

    statements = (
        f"""
        INSERT INTO {alert_tab} (object_id,
                                 alert_date,
                                 point_count,
                                 satellite_base,
                                 cluster)
            SELECT
                peat_un_id,
                date_time,
                point_count,
                'https://apps.sentinel-hub.com/eo-browser/?zoom=14&lat='
                    || ST_Y(
                        ST_Transform(
                            ST_Centroid(buffer)::geometry,4326)::geometry)
                    || '&lng='
                    || ST_X(
                        ST_Transform(
                            ST_Centroid(buffer)::geometry,4326)::geometry)
                    || '&themeId=DEFAULT-THEME',
                buffer
            FROM {clust_view}
            WHERE
                date_time >= (TIMESTAMP 'today' - INTERVAL '{period}')
                AND date_time < TIMESTAMP 'today'
        """,
        f"""
        UPDATE {alert_tab}
            SET
                alert_date = '{cur_date}',
                source = 'Робот'
            WHERE
                alert_date >= (TIMESTAMP 'today' - INTERVAL '{period}')
                AND alert_date < TIMESTAMP 'today'
                AND source IS NULL
        """
    )

    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        logger.info("Adding alerts finished.")
    except IOError as err:
        logger.error(f"Error adding alerts: {err}")

    close_conn(conn, cursor)

    # cursor.execute("SELECT count(*) FROM %s"%(subs_tab))
    # return cursor.fetchone()[0]


def get_zone_ids_for_region(reglist):
    """Generate a list of oopt ids for regions in reglist."""
    logger.info("Making OOPT ids list for regions...")
    [oopt_zones] = get_config("tables", ["oopt_zones"])
    conn, cursor = get_cursor()
    full_oopt_lst = ""
    for reg in reglist:
        logger.debug(f"Region: --{reg}--.")
        cursor.execute(f"""SELECT id
                           FROM {oopt_zones}
                           WHERE region LIKE '%{reg}%'""")
        oopt_ids = cursor.fetchall()
        oopt_lst = ""
        for elem in oopt_ids:
            oopt_lst += f"{str(elem[0])},"
            # oopt_lst.append(elem[0])
        full_oopt_lst += oopt_lst[0:-1]
    # logger.debug(f"Zones list: {full_oopt_lst}.")
    return full_oopt_lst


def get_zone_ids_for_ecoregion(reglist):
    """Generate a list of oopt ids for ecoregions in reglist."""
    logger.info("Making OOPT ids list for ecoregions...")
    [oopt_zones] = get_config("tables", ["oopt_zones"])
    conn, cursor = get_cursor()
    full_oopt_lst = ""
    for reg in reglist:
        logger.debug(f"Region: --{reg}--.")
        cursor.execute(f"""SELECT id
                           FROM {oopt_zones}
                           WHERE ecoregion LIKE '%{reg}%'""")
        oopt_ids = cursor.fetchall()
        oopt_lst = ""
        for elem in oopt_ids:
            oopt_lst += f"{str(elem[0])},"
        full_oopt_lst += oopt_lst[0:-1]
    return full_oopt_lst


def check_reg_stat(reg, period, critical):
    """Check full points count and count of critical points for region."""
    # logger.info("Getting statistic for %s..."%(reg))
    [year_tab] = get_config("tables", ["year_tab"])
    conn, cursor = get_cursor()

    statements = (
        f"""
        SELECT count(*) FROM
            (SELECT name
             FROM {year_tab}
             WHERE
                date_time >= TIMESTAMP 'today' - INTERVAL '{period}'
                AND date_time < TIMESTAMP 'today'
                AND critical >= {critical}
                AND region = '{reg}') as critical_sel
        """,
        f"""
        SELECT count(*) FROM
            (SELECT name
             FROM {year_tab}
             WHERE
                date_time >= TIMESTAMP 'today' - INTERVAL '{period}'
                AND date_time < TIMESTAMP 'today'
                AND region = '{reg}') as all_sel
        """
    )
    try:
        cursor.execute(statements[0])
        critical_cnt = cursor.fetchone()[0]
        cursor.execute(statements[1])
        all_cnt = cursor.fetchone()[0]
        logger.info(f"Finished for:{reg}. All - {all_cnt}, "
                    f"critical - {critical_cnt}")
    except IOError as err:
        logger.error(f"Error getting statistic for region: {err}")

    close_conn(conn, cursor)

    return critical_cnt, all_cnt


def make_tlg_peat_stat_msg(reg_list, period, limit):
    """Generate a peat-point statistic message for sending over telegramm."""
    if limit is None:
        limit = 0
    if period is None:
        period = 24
    full_cnt = 0
    full_cr_cnt = 0
    msg = "Количество точек:"
    for reg in reg_list:
        critical_cnt, all_cnt = check_reg_stat(reg, period, limit)
        if all_cnt > 0:
            msg += f"\r\n{reg}: {all_cnt}"
            if critical_cnt > 0:
                msg += f"\r\nкритичных: {critical_cnt}"
        full_cnt += all_cnt
        full_cr_cnt += critical_cnt
    if full_cnt == 0:
        msg = "Нет новых точек."
    return msg