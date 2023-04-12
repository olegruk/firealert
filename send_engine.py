"""
Main firealert robot part.

Started via crontab: '15 * * * * send_engine.py'

Perform a main service for organize subscribtions and send data for users.

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
import psycopg2
from faservice import (
    get_config,
    get_tuple_cursor,
    close_conn,
    get_path,
    smf_new_topic,
    str_to_lst)
from faservice import (
    write_to_kml,
    write_to_yadisk,
    send_email_with_attachment,
    send_email_message,
    send_doc_to_telegram,
    send_to_telegram,
    points_tail)
from requester import (
    make_tlg_peat_stat_msg,
    make_zone_stat_msg,
    make_smf_stat_msg,
    get_oopt_ids_for_region,
    get_oopt_ids_for_ecoregion,
    new_alerts)
from mylogger import init_logger

logger = init_logger()


def make_subs_table1(conn, cursor, src_tab, crit_or_peat, limit, period,
                    reg_list, whom, is_incremental, filter_tech):
    """Create a table with firepoints for subscriber."""
    logger.info(f"Creating table for subs_id: {whom}...")
    subs_tab = f"for_s{str(whom)}"
    marker = f"[s{str(whom)}]"
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
                peat_id VARCHAR(256),
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
                                peat_id,
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
                {src_tab}.peat_id,
                {src_tab}.peat_district,
                {src_tab}.peat_class,
                {src_tab}.peat_fire,
                {src_tab}.geog
            FROM {src_tab}
            WHERE
                date_time >= TIMESTAMP 'now' - INTERVAL '{period}'
                AND date_time < TIMESTAMP 'now'
                AND {crit_or_peat} >= {limit}
                AND region in {reg_list}
                AND NOT(
                    (tech IS NOT NULL)
                    AND NOT (
                        {filter_tech}
                        AND (tech IS NOT NULL)
                        )
                    )
            ORDER BY {src_tab}.peat_id
        """,
        f"""
        UPDATE {src_tab}
            SET
                whom = whom || '{marker}'
            WHERE
                date_time >= TIMESTAMP 'now' - INTERVAL '{period}'
                AND date_time < TIMESTAMP 'now'
                AND {crit_or_peat} >= {limit}
                AND region in {reg_list}
                AND POSITION('{marker}' in whom) = 0
        """,
        f"""
        UPDATE {src_tab}
            SET
                whom = '{marker}'
            WHERE
                date_time >= TIMESTAMP 'now' - INTERVAL '{period}'
                AND date_time < TIMESTAMP 'now'
                AND {crit_or_peat} >= {limit}
                AND region in {reg_list}
                AND whom is Null
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
                    'Торфяник (ID): ' || peat_id || '\n' ||
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
                peat_id IS NULL
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
                                region,geog)
            SELECT
                {src_tab}.acq_date,
                {src_tab}.acq_time,
                {src_tab}.latitude,
                {src_tab}.longitude,
                {src_tab}.satellite,
                {src_tab}.region,
                {src_tab}.geog
            FROM {src_tab}
            WHERE
                country = 'Россия'
                AND date_time >= TIMESTAMP 'now' - INTERVAL '{period}'
                AND date_time < TIMESTAMP 'now'
                AND NOT(
                    (tech IS NOT NULL)
                    AND NOT (
                        {filter_tech}
                        AND (tech IS NOT NULL)
                    )
                )
        """,
        f"""
        UPDATE {src_tab}
            SET
                whom = whom || '{marker}'
            WHERE
                country = 'Россия'
                AND date_time >= TIMESTAMP 'now' - INTERVAL '{period}'
                AND date_time < TIMESTAMP 'now'
                AND POSITION('{marker}' in whom) = 0
        """,
        f"""
        UPDATE {src_tab}
            SET
                whom = '{marker}'
            WHERE
                country = 'Россия'
                AND date_time >= TIMESTAMP 'now' - INTERVAL '{period}'
                AND date_time < TIMESTAMP 'now'
                AND whom is Null
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

    statements_regional_incremental = (
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
                peat_id VARCHAR(256),
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
                                peat_id,
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
                {src_tab}.peat_id,
                {src_tab}.peat_district,
                {src_tab}.peat_class,
                {src_tab}.peat_fire,
                {src_tab}.geog
            FROM {src_tab}
            WHERE
                date_time > TIMESTAMP 'today'
                AND {crit_or_peat} >= {limit}
                AND region in {reg_list}
                AND (
                    whom is Null
                    OR POSITION('{marker}' in whom) = 0)
                AND NOT(
                    (tech IS NOT NULL)
                    AND NOT (
                        {filter_tech}
                        AND (tech IS NOT NULL)
                    )
                )
            ORDER BY {src_tab}.peat_id
        """,
        f"""
        UPDATE {src_tab}
            SET
                whom = whom || '{marker}'
            WHERE
                date_time > TIMESTAMP 'today'
                AND {crit_or_peat} >= {limit}
                AND region in {reg_list}
                AND POSITION('{marker}' in whom) = 0
        """,
        f"""
        UPDATE {src_tab}
            SET
                whom = '{marker}'
            WHERE
                date_time > TIMESTAMP 'today'
                AND {crit_or_peat} >= {limit}
                AND region in {reg_list}
                AND whom is Null
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
                    'Торфяник (ID): ' || peat_id || '\n' ||
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
                peat_id IS NULL
        """
    )

    statements_allrussia_incremental = (
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
            FROM {src_tab}
            WHERE
                country = 'Россия'
                AND date_time > TIMESTAMP 'today'
                AND (
                    whom is Null
                    OR POSITION('{marker}' in whom) = 0
                )
                AND NOT(
                    (tech IS NOT NULL)
                    AND NOT (
                        {filter_tech}
                        AND (tech IS NOT NULL)
                    )
                )
        """,
        f"""
        UPDATE {src_tab}
            SET
                whom = whom || '{marker}'
            WHERE
                country = 'Россия'
                AND date_time > TIMESTAMP 'today'
                AND POSITION('{marker}' in whom) = 0
        """,
        f"""
        UPDATE {src_tab}
            SET
                whom = '{marker}'
            WHERE
                country = 'Россия'
                AND date_time > TIMESTAMP 'today'
                AND whom is Null
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
        logger.info(f"The table created: subs_id: {whom}")
    except psycopg2.OperationalError as err:
        logger.error(f"Error creating subscribers tables: {err}")
        return -1
    cursor.execute(f"SELECT count(*) FROM {subs_tab}")
    return cursor.fetchone()[0]


def make_subs_oopt_table(conn, cursor, year_tab, oopt_tab, oopt_ids,
                         period, whom, filter_tech):
    """Create OOPT-table fo subscriber with ID 'subs_id'."""
    logger.info(f"Creating oopt table for subs_id: {whom}...")
    subs_tab = f"for_s{str(whom)}"
    marker = f"[s{str(whom)}]"
    period = f"{period} hours"

    statements = (
        f"""
        DROP TABLE IF EXISTS {subs_tab}
        """,
        f"""
        CREATE TABLE {subs_tab} AS
            SELECT
                acq_date AS date,
                acq_time AS time,
                latitude,
                longitude,
                region,
                oopt,
                oopt_id,
                geog
            FROM {year_tab}
            WHERE
                date_time >= TIMESTAMP 'now' - INTERVAL '{period}'
                AND oopt_id IN ({oopt_ids})
                AND (
                    whom is Null
                    OR POSITION('{marker}' in whom) = 0
                )
                AND NOT (
                    {filter_tech}
                    AND (tech IS NOT NULL)
                )
            ORDER BY oopt_id
        """,
        f"""
        UPDATE {year_tab}
            SET
                whom = whom || '{marker}'
            WHERE
                date_time > TIMESTAMP 'now' - INTERVAL '{period}'
                AND oopt_id IN ({oopt_ids})
                AND POSITION('{marker}' in whom) = 0
                AND NOT (
                    {filter_tech}
                    AND (tech IS NOT NULL)
                )
        """,
        f"""
        UPDATE {year_tab}
            SET
                whom = '{marker}'
            WHERE
                date_time > TIMESTAMP 'now' - INTERVAL '{period}'
                AND oopt_id IN ({oopt_ids})
                AND whom is Null
                AND NOT (
                    {filter_tech}
                    AND (tech IS NOT NULL)
                )
        """,
        f"""
        ALTER TABLE {subs_tab}
            ADD COLUMN description VARCHAR(500)
        """,
        f"""
        UPDATE {subs_tab}
            SET
                oopt = {oopt_tab}.name
            FROM {oopt_tab}
            WHERE
                {subs_tab}.oopt_id = {oopt_tab}.id
        """,
        f"""
        UPDATE {subs_tab}
            SET
                description =
                    'Дата: ' || date || '\n' ||
                    'Время: ' || time || '\n' ||
                    'Широта: ' || latitude || '\n' ||
                    'Долгота (ID): ' || longitude || '\n' ||
                    'Регион: ' || region || '\n' ||
                    'ООПТ: ' || oopt
        """
    )

    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        logger.info(f"The table created: subs_id: {whom}")
    except psycopg2.OperationalError as err:
        logger.error(f"Error creating subscribers tables: {err}")
    cursor.execute(f"""SELECT
                            oopt_id,
                            region,
                            oopt,
                            count(*)
                     FROM {subs_tab}
                     GROUP BY oopt_id, region, oopt""")
    oopt_tab = cursor.fetchall()
    cursor.execute(f"""SELECT
                            ST_XMax(ST_Extent(geog::geometry)) as x_max,
                            ST_YMax(ST_Extent(geog::geometry)) as y_max,
                            ST_XMin(ST_Extent(geog::geometry)) as x_min,
                            ST_YMin(ST_Extent(geog::geometry)) as y_min,
                            ST_Extent(geog::geometry) AS subs_extent
                       FROM {subs_tab}""")
    oopt_extent = cursor.fetchone()
    return oopt_tab, oopt_extent


def make_subs_table(conn, cursor, year_tab, zones_tab, id_list,
                         period, whom, filter_tech, zone_type):
    """Create table with points for subscriber with ID 'subs_id'."""
    logger.info(f"Creating table for subs_id: {whom}...")
    subs_tab = f"for_s{whom}"
    marker = f"[s{whom}]"
    period = f"{period} hours"

    statements = (
        f"""
        DROP TABLE IF EXISTS {subs_tab}
        """,
        f"""
        CREATE TABLE {subs_tab} AS
            SELECT
                acq_date AS date,
                acq_time AS time,
                latitude,
                longitude,
                region,
                {zone_type}_id,
                geog
            FROM {year_tab}
            WHERE
                date_time >= TIMESTAMP 'now' - INTERVAL '{period}'
                AND {zone_type}_id IN ({id_list})
                AND (
                    whom is Null
                    OR POSITION('{marker}' in whom) = 0
                )
                AND NOT (
                    {filter_tech}
                    AND (tech_id IS NOT NULL)
                )
            ORDER BY {zone_type}_id
        """,
        f"""
        UPDATE {year_tab}
            SET
                whom = whom || '{marker}'
            WHERE
                date_time > TIMESTAMP 'now' - INTERVAL '{period}'
                AND {zone_type}_id IN ({id_list})
                AND POSITION('{marker}' in whom) = 0
                AND NOT (
                    {filter_tech}
                    AND (tech_id IS NOT NULL)
                )
        """,
        f"""
        UPDATE {year_tab}
            SET
                whom = '{marker}'
            WHERE
                date_time > TIMESTAMP 'now' - INTERVAL '{period}'
                AND {zone_type}_id IN ({id_list})
                AND whom is Null
                AND NOT (
                    {filter_tech}
                    AND (tech_id IS NOT NULL)
                )
        """,
        f"""
        ALTER TABLE {subs_tab}
            ADD COLUMN description VARCHAR(500)
            ADD COLUMN {zone_type} VARCHAR(100)
        """,
        f"""
        UPDATE {subs_tab}
            SET
                {zone_type} = {zones_tab}.name
            FROM {zones_tab}
            WHERE
                {subs_tab}.{zone_type}_id = {zones_tab}.id
        """,
        f"""
        UPDATE {subs_tab}
            SET
                description =
                    'Дата: ' || date || '\n' ||
                    'Время: ' || time || '\n' ||
                    'Широта: ' || latitude || '\n' ||
                    'Долгота (ID): ' || longitude || '\n' ||
                    'Регион: ' || region || '\n' ||
                    'Территория: ' || {zone_type}
        """
    )

    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        logger.info(f"The table created for subs_id: {whom}")
    except psycopg2.OperationalError as err:
        logger.error(f"Error creating subscribers tables: {err}")
    cursor.execute(f"""SELECT
                            {zone_type}_id,
                            region,
                            {zone_type},
                            count(*)
                     FROM {subs_tab}
                     GROUP BY {zone_type}_id, region, {zone_type}""")
    res_tab = cursor.fetchall()
    cursor.execute(f"""SELECT
                            ST_XMax(ST_Extent(geog::geometry)) as x_max,
                            ST_YMax(ST_Extent(geog::geometry)) as y_max,
                            ST_XMin(ST_Extent(geog::geometry)) as x_min,
                            ST_YMin(ST_Extent(geog::geometry)) as y_min,
                            ST_Extent(geog::geometry) AS subs_extent
                       FROM {subs_tab}""")
    points_extent = cursor.fetchone()
    return res_tab, points_extent


def drop_whom_table(conn, cursor, whom):
    """Delete temporary subscribers tablles."""
    subs_tab = f"for_s{str(whom)}"
    logger.info(f"Dropping table {subs_tab}")
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {subs_tab}")
        conn.commit()
        logger.info("Table dropped.")
    except psycopg2.OperationalError as err:
        logger.error(f"Error dropping table: {err}")


def fill_send_times(conn, cursor, subs_tab, subs_id, zero_time, period):
    """Fill sending times field in subscribers table (if not filled)."""
    logger.info(f"Creating send times for {subs_id}.")
    if period is None:
        period = 24
    if zero_time is None:
        zero_time = "00:15"
    zero_hour = int(zero_time.split(":")[0])
    send_hours = ""
    if 24 % period == 0:
        num_of_times = int(24//period)
    else:
        num_of_times = int(24//period) + 1
    for i in range(num_of_times):
        new_hour = (zero_hour + i*period) % 24
        if new_hour < 10:
            send_hours += f"0{str(new_hour)}"
        else:
            send_hours += str(new_hour)
        if i < num_of_times - 1:
            send_hours += ","
    cursor.execute(f"""UPDATE {subs_tab}
                       SET
                            send_times = '{send_hours}'
                       WHERE
                            subs_id = {subs_id}
                    """)
    conn.commit()
    return send_hours


def make_file_name(period, date, whom, result_dir, iter):
    """Make full name of file wich will sended to subscriber."""
    if iter == 0:
        suff = ""
    else:
        suff = f"_inc{str(iter)}"
    if period == 24:
        dst_file_name = f"{date}_{whom}{suff}.kml"
    else:
        period_mod = period
        period_mod = period_mod.replace(" ", "_")
        dst_file_name = f"{date}_{whom}_{period_mod}{suff}.kml"
    dst_file = os.path.join(result_dir, dst_file_name)
    if os.path.isfile(dst_file):
        drop_temp_file(dst_file)
    return dst_file_name


def drop_temp_files(result_dir):
    """Drop temporary files in 'result_dir'."""
    for the_file in os.listdir(result_dir):
        file_path = os.path.join(result_dir, the_file)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
            # elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as err:
            logger.error(f"Cannot remove files: {err}")


def drop_temp_file(the_file):
    """Drop one temporary file 'the_file'."""
    logger.info("Dropping temp files...")
    try:
        if os.path.isfile(the_file):
            os.remove(the_file)
    except Exception as err:
        logger.error(f"Cannot remove files: {err}")


def make_mail_attr(date, period, num_points):
    """Make subject and text of email for subscriber."""
    if period == 24:
        if num_points > 0:
            subject = f"Daily points per {date} ({num_points} points)"
        else:
            subject = f"Daily points per {date} (no any points)"
        body_text = "In the attachment firepoints for last day.\r\n"\
                    "Email to dist_mon@firevolonter.ru if you find any "\
                    "errors or inaccuracies."
    else:
        if num_points > 0:
            subject = f"Points per last {period} ({num_points} points)"
        else:
            subject = f"Points per last {period} (no any points)"
        body_text = "In the attachment firepoints for last days.\r\n"\
                    "Email to dist_mon@firevolonter.ru if you find any "\
                    "errors or inaccuracies."
    return subject, body_text


def set_name(conn, cursor, subs_tab, subs_id):
    """Make subscribers name? based on his ID."""
    logger.info(f"Setting name for ID {subs_id}")
    new_name = f"s-{str(subs_id)}"
    cursor.execute(f"""UPDATE {subs_tab}
                       SET
                            subs_name = '{new_name}'
                       WHERE
                            subs_id = {subs_id}
                    """)
    conn.commit()
    logger.info("Setting name done.")


def make_zone_msg(stat, extent, zone_type):
    """Generate a statistic message for zones to sending over telegram."""
    if zone_type == "oopt":
        intro = "ООПТ"
    elif zone_type == "peat":
        intro = "торфяниках"
    elif zone_type == "ctrl":
        intro = "зонах особого контроля"
    elif zone_type == "oopt_buffer":
        intro = "буферных зонах ООПТ"
    elif zone_type == "peat_buffer":
        intro = "буферных зонах торфяников"
    elif zone_type == "ctrl_buffer":
        intro = "буферах зон особого контроля"
    else:
        intro = "непонятных зонах"
    full_cnt = 0
    msg = f"Новые точки в {intro}:"
    for st_str in stat:
        # Статистика с индексами ООПТ
        # msg += f"\r\n{st_str[1]} "\
        #        f"- {st_str[2]} ({st_str[0]}): {st_str[3]}"
        # Статистика без индексов ООПТ
        msg += f"\r\n{st_str[1]} - {st_str[2]}: {st_str[3]}"
        full_cnt += st_str[3]
    x_max = extent[0]
    y_max = extent[1]
    x_min = extent[2]
    y_min = extent[3]
    msg += (f"\n\n"
            f"<a href="
            f"'https://maps.wwf.ru/portal/apps/webappviewer/"
            f"index.html?id=b1d52f160ac54c3faefd4592da4cf8ba"
            f"&extent={x_min},{y_min},{x_max},{y_max}'"
            f">Посмотреть на карте...</a>")
    return msg


def make_subs_kml(point_period,
                    subs_name,
                    subs_id,
                    result_dir,
                    date,
                    int_now_hour):
    """Writing kml-file for subscriber."""
    logger.info("Creating kml file...")
    dst_file_name = make_file_name(point_period,
                                    date,
                                    subs_name,
                                    result_dir,
                                    int_now_hour)
    dst_file = os.path.join(result_dir, dst_file_name)
    write_to_kml(dst_file, subs.subs_id)
    return dst_file


def send_email_to_subs(subs_email, subs_point_period, date, full_cnt, dst_file):
    """Sending to subscriber a kml-file over email."""
    if (subs.email is not None and subs.email != ''):
        logger.info("Creating maillist...")
        maillist = subs.email.replace(" ", "").split(",")
        subject, body_text = make_mail_attr(date,
                                            subs.point_period,
                                            full_cnt)
        try:
            send_email_with_attachment(maillist,
                                    subject,
                                    body_text,
                                    [dst_file])
        except IOError as err:
            logger.error(f"Error seneding e-mail. "
                            f"Error: {err}")
    else:
        logger.warning("Unable to send email. Empty maillist.")


def send_tlg_to_subs(subs_telegramm, dst_file, url, full_cnt):
    if subs_telegramm is not None and subs_telegramm != '':
        doc = open(dst_file, "rb")
        send_doc_to_telegram(url, subs.telegramm, doc)
        tail = points_tail(full_cnt)
        send_to_telegram(url, subs.telegramm, f"В файле {full_cnt} {tail}.")
    else:
        logger.warning("Unable to send telegram message. Empty telegram_id..")


def make_zones_list(zones, regions, ecoregions):
    zone_list = []
    if (zones is not None) and (zones != ''):
        zone_list = zones
    elif (regions is not None) and (regions != ''):
        zone_list = get_zone_ids_for_region(regions)
    elif (ecoregions is not None) and (ecoregions != ''):
        zone_list = get_zone_ids_for_ecoregion(ecoregions)
    return zone_list


def filter_zones(zonelist, peatfire, zones_tab):
    cursor.execute(f"""SELECT id 
                       FROM {zones_tab}
                       WHERE 
                            category = 'торфяник'
                            AND critical >= {peatfire}
                    """)
    relevant_zones = cursor.fetchall()
    cutted_zonelist = []
    for zone in zonelist:
        if zone in relevant_zones:
            cutted_zonelist.append(zone)
    return cutted_zonelist

def send_to_subscribers_job():
    """Send to subscribers job main function."""
    logger.info("---------------------------------")
    logger.info("Process [send_engine.py] started.")

    currtime = time.localtime()
    date = time.strftime("%Y-%m-%d", currtime)
    now_hour = time.strftime("%H", currtime)

    [year_tab, subs_tab, zones_tab] = get_config("tables", ["year_tab",
                                                           "subs_tab",
                                                           "oopt_zones"])
    [data_root, temp_folder] = get_config("path", ["data_root",
                                                   "temp_folder"])
    [to_dir] = get_config("yadisk", ["yadisk_out_path"])
    [url] = get_config("telegramm", ["url"])
    [outline] = get_config("tables", ["vip_zones"])
    [alerts_check_time] = get_config("alerts", ["check_time"])
    [smf_check_time] = get_config("smf", ["check_time"])
    conn, cursor = get_tuple_cursor()

    # Загружаем данные о подписчиках
    # subs_id           serial          автоидентификатор
    # subs_name         varchar(10)     имя подписчика, для удобства
    #                                       ориентирования в подписках
    # active            boolean         подписка активна?
    # regions           varchar()       список регионов на контроле подписчика
    # email             varchar()       список e-mail адресов подписчика
    # telegramm         varchar(20)     список телеграмм-чатов подписчика
    # email_stat        boolean         слать статистику по почте?
    # teleg_stat        boolean         слать статистику в телеграмм?
    # email_point       boolean         слать точки по почте?
    # teleg_point       boolean         слать точки в телеграмм?
    # stat_period       integer         период в часах, за который
    #                                       выдается статистика
    # point_period      integer         период в часах, за который
    #                                       выбираются точки
    # crit_or_fire      varchar(4)      критерий отбора точек,
    #                                       критичность точки - 'crit'
    #                                       или горимость торфа - 'fire'
    # critical          integer         порог критичности точки
    #                                       для отбора точек
    # peatfire          integer         порог горимости торфяника
    #                                       для отбора точек
    # send_first_time   varchar(5)      время первой рассылки за сутки
    # send_period       integer         периодичность рассылки в часах
    # send_times        varchar()       список временных меток для рассылки
    #                                       (в какие часы делается рассылка)
    # vip_zones         boolean         рассылать ли информацию
    #                                       по зонам особого внимания?
    # send_empty        boolean         отправлять или нет пустой файл?
    # ya_disk           boolean         писать или нет файл на яндекс-диск
    # zones             varchar()       список зон особого внимания
    #                                       на контроле подписчика
    # filter_tech       boolean         фильтровать техноген?
    # oopt_zones        varchar()       список ООПТ на контроле подписчика
    # check_oopt        boolean         проверять попадание в ООПТ?
    # oopt_regions      varchar()       список регионов для контроля ООПТ
    #                                       (читается, если пуст явный список)
    # check_oopt_buf    boolean         проверять попадание
    #                                       в буферные зоны ООПТ?

    cursor.execute(f"SELECT * FROM {subs_tab} WHERE active")
    subscribers = cursor.fetchall()

    # Создаем каталог для записи временных файлов
    result_dir = get_path(data_root, temp_folder)

    for subs in subscribers:
        if (subs.subs_name is None or subs.subs_name == ''):
            set_name(conn, cursor, subs_tab, subs.subs_id)
        logger.info(f"Processing for {subs.subs_name}...")
        if (subs.send_times is None or subs.send_times == ''):
            logger.warning(f"Empty send times list for {subs.subs_name}.")
            continue
        else:
            sendtimelist = subs.send_times.split(",")

        if now_hour in sendtimelist:
            zone_list = make_zones_list(subs.zones,
                                        subs.regions,
                                        subs.ecoregions)
            if zone_list == []:
                logger.warning("Empty zones list for {subs.subs_name}.")
                continue

#-------------------------------------------------------------------------------
            msg = make_tlg_peat_stat_msg(reg_list, period, subs.critical)
#-------------------------------------------------------------------------------


            for zone_type in subs.zone_types:
                if zone_type == "peat":
                    zone_list = filter_zones(zonelist, subs.critical, zones_tab)
                logger.info(f"Check {zone_type} points now.")
                stat, extent = make_subs_table(conn,
                                                cursor,
                                                year_tab,
                                                zones_tab,
                                                zone_list,
                                                subs.period,
                                                subs.subs_id,
                                                subs.filter_tech,
                                                zone_type)
                num_points = len(stat)
                if (num_points > 0) and (subs.teleg_stat or subs.email_stat):
                    msg = make_zone_msg(stat, extent, zone_type)
                    if subs.teleg_stat:
                        logger.info("Sending stat to telegram...")
                        send_to_telegram(url, subs.telegramm, msg)
                    if subs.email_stat:
                        logger.info("Sending stat to email...")
                        subject = f"Statistic per last {subs.stat_period}"
                        maillist = subs.email.replace(" ", "").split(",")
                        send_email_message(maillist, subject, msg)
                if (subs.email_point or subs.teleg_point):
                    if (num_points > 0) or subs.send_empty:
                        dst_file = make_subs_kml(subs.period,
                                                    subs.subs_name,
                                                    subs.subs_id,
                                                    result_dir,
                                                    date,
                                                    int(now_hour))
                        if subs.email_point:
                            send_email_to_subs(subs.email,
                                                subs.period,
                                                date,
                                                num_points,
                                                dst_file)
                        if subs.teleg_point:
                            send_tlg_to_subs(subs.telegramm,
                                                dst_file,
                                                url,
                                                num_points)
                        drop_temp_file(dst_file)
                    else:
                        logger.info(f"Don`t send zero-point {zone_type} file.")
                logger.info("Dropping tables...")
                drop_whom_table(conn,cursor,subs.subs_id)
                if subs.check_buffers:
                    logger.info(f"Check {zone_type} buffers points now.")
                    stat, extent = make_subs_table(conn,
                                                    cursor,
                                                    year_tab,
                                                    buffers_tab,
                                                    zone_list,
                                                    subs.period,
                                                    subs.subs_id,
                                                    subs.filter_tech,
                                                    zone_type)
                    num_points = len(stat)
                    if num_points > 0:
                        msg = make_zone_msg(stat, extent, f"{zone_type}_buffer")
                        send_to_telegram(url, subs.telegramm, msg)
                    if (subs.email_point or subs.teleg_point):
                        if (num_points > 0) or subs.send_empty:
                            dst_file = make_subs_kml(subs.period,
                                                        subs.subs_name,
                                                        subs.subs_id,
                                                        result_dir,
                                                        date,
                                                        int(now_hour))
                            if subs.email_point:
                                send_email_to_subs(subs.email,
                                                    subs.period,
                                                    date,
                                                    num_points,
                                                    dst_file)
                            if subs.teleg_point:
                                send_tlg_to_subs(subs.telegramm,
                                                    dst_file,
                                                    url,
                                                    num_points)
                            drop_temp_file(dst_file)
                        else:
                            logger.info(f"Don`t send zero-point "\
                                        f"{zone_type} buffers file.")
                    logger.info("Dropping tables...")
                    drop_whom_table(conn,cursor,subs.subs_id)
            if now_hour == sendtimelist[0] and subs.ya_disk:
                logger.info("Writing to yadisk...")
                subs_folder = f"for_s{str(subs.subs_name)}"
                write_to_yadisk(dst_file_name, result_dir, to_dir, subs_folder)
            logger.info("Dropping tables...")
            drop_whom_table(conn, cursor, subs.subs_id)
        else:
            logger.info("Do anything? It`s not time yet!")

    if now_hour == alerts_check_time:
        [alerts_period] = get_config("alerts", ["period"])
        new_alerts(alerts_period, date)

#-------------------------------------------------------------------------------


#-------------------------------------------------------------------------------
    close_conn(conn, cursor)
    logger.info("Process [send_engine.py] stopped.")


if __name__ == "__main__":
    send_to_subscribers_job()
