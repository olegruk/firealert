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
    get_path)
from faservice import (
    write_to_kml,
    write_to_yadisk,
    send_email_with_attachment,
    send_email_message,
    send_doc_to_telegram,
    send_to_telegram,
    points_tail)
from requester import (
    get_zone_ids_for_region,
    get_zone_ids_for_ecoregion,
    new_alerts)
from mylogger import init_logger

logger = init_logger(loglevel="Debug")


def count_crit_points(cursor, zones_tab, zone_id, limit):
    """Check count of critical points for zonelist."""
    # logger.info("Getting statistic for %s..."%(reg))
    if limit is not None:
        stat = (
            f"""
            SELECT count(*)
            FROM {zones_tab}
            WHERE id = {zone_id}
                AND critical >= {limit}
            """
        )
        try:
            cursor.execute(stat)
            critical_cnt = cursor.fetchone()[0]
        except psycopg2.Error as err:
            logger.error(f"Error getting critical statistic for zone: {err}")
            critical_cnt = 0
    else:
        logger.warning(f"Critical limit is Null. Count of points setted to 0.")
        critical_cnt = 0
    return critical_cnt


def make_subs_table(conn, cursor, year_tab, zones_tab, buffers_tab, id_list,
                    ch_buf, period, whom, filter_tech, zone_type, limit):
    """Create table with points for subscriber with ID 'subs_id'."""
    logger.info(f"Creating table for subs_id: {whom}...")
    subs_tab = f"for_s{whom}"
    marker = f"[s{whom}]"
    period = f"{period} hours"

    stat_non_buf = (
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
                critical,
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
            ADD COLUMN description VARCHAR(500),
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

    stat_buf = (
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
                COALESCE({zone_type}_id, {zone_type}_buf_id) AS {zone_type}_id,
                critical,
                distance,
                geog
            FROM {year_tab}
            WHERE
                date_time >= TIMESTAMP 'now' - INTERVAL '{period}'
                AND ({zone_type}_id IN ({id_list})
                     OR {zone_type}_buf_id IN ({id_list}))
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
                AND ({zone_type}_id IN ({id_list})
                     OR {zone_type}_buf_id IN ({id_list}))
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
                AND ({zone_type}_id IN ({id_list})
                     OR {zone_type}_buf_id IN ({id_list}))
                AND whom is Null
                AND NOT (
                    {filter_tech}
                    AND (tech_id IS NOT NULL)
                )
        """,
        f"""
        ALTER TABLE {subs_tab}
            ADD COLUMN description VARCHAR(500),
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

    if ch_buf:
        statements = stat_buf
    else:
        statements = stat_non_buf

    try:
        logger.debug(f"The period is: {period}")
        n = 0
        for sql_stat in statements:
            n += 1
            logger.debug(f"Execute statement #: {n}")
            logger.debug(f"Zone_type: {zone_type}")
            # logger.debug(f"Id_list: {id_list}")
            logger.debug(f"Marker: {marker}")
            logger.debug(f"Filter_tech: {filter_tech}")
            logger.debug(f"Period: {period}")
            logger.debug(f"Is buffer: {ch_buf}")
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
    num = 0
    for str in res_tab:
        num += str[3]
    return num, res_tab, points_extent


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
                    "Email to firealert.robot@yandex.ru if you find any "\
                    "errors or inaccuracies."
    else:
        if num_points > 0:
            subject = f"Points per last {period} ({num_points} points)"
        else:
            subject = f"Points per last {period} (no any points)"
        body_text = "In the attachment firepoints for last days.\r\n"\
                    "Email to firealert.robot@yandex.ru if you find any "\
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


def make_zone_msg(cursor, zones_tab, limit, stat, extent, zone_type):
    """Generate a statistic message for zones to sending over telegram."""
    if zone_type == "oopt":
        intro = "ООПТ"
    elif zone_type == "peat":
        intro = "торфяниках"
    elif zone_type == "ctrl":
        intro = "зонах особого контроля"
    elif zone_type == "safe":
        intro = "охранных зонах"
    # elif zone_type == "oopt_buffer":
        # intro = "буферных зонах ООПТ"
    # elif zone_type == "peat_buffer":
        # intro = "буферных зонах торфяников"
    # elif zone_type == "ctrl_buffer":
        # intro = "буферах зон особого контроля"
    # elif zone_type == "safe_buffer":
        # intro = "буферах охранных зон"
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
    msg += f"\nВсего точек: {full_cnt}"
    crit_count = count_crit_points(cursor, zones_tab, st_str[0], limit)
    if crit_count > 0:
        msg += f"\nВысокой опасности: {crit_count}"
    x_max = extent[0]
    y_max = extent[1]
    x_min = extent[2]
    y_min = extent[3]
    """
    if (x_max != x_min) and (y_max != y_min):
        msg += (f"\n\n"
                f"<a href="
                f"'https://maps.rumap.ru/portal/apps/webappviewer/"
                f"index.html?id=b1d52f160ac54c3faefd4592da4cf8ba"
                f"&extent={x_min},{y_min},{x_max},{y_max}'"
                f">Посмотреть на карте...</a>")
    else:
        msg += (f"\n\n"
                f"<a href="
                f"'https://maps.rumap.ru/portal/apps/webappviewer/"
                f"index.html?id=b1d52f160ac54c3faefd4592da4cf8ba"
                f"&extent={x_min},{y_min},{x_max},{y_max}&level=9'"
                f">Посмотреть на карте...</a>")
    """
    return msg


def make_subs_kml(point_period,
                  subs_name,
                  subs_id,
                  result_dir,
                  date,
                  int_now_hour,
                  critical):
    """Write kml-file for subscriber."""
    logger.info("Creating kml file...")
    dst_file_name = make_file_name(point_period,
                                   date,
                                   subs_name,
                                   result_dir,
                                   int_now_hour)
    dst_file = os.path.join(result_dir, dst_file_name)
    write_to_kml(dst_file, subs_id, critical)
    return dst_file


def send_email_to_subs(subs_emails, subs_point_period,
                       date, full_cnt, dst_file):
    """Send to subscriber a kml-file over email."""
    if (subs_emails is not None and subs_emails != ''):
        logger.info("Creating maillist...")
        maillist = subs_emails.replace(" ", "").split(",")
        subject, body_text = make_mail_attr(date,
                                            subs_point_period,
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


def send_tlg_to_subs(subs_tlg_id, dst_file, url, full_cnt):
    """Send file with hotspots to telegram."""
    if subs_tlg_id is not None and subs_tlg_id != '':
        doc = open(dst_file, "rb")
        send_doc_to_telegram(url, subs_tlg_id, doc)
        tail = points_tail(full_cnt)
        send_to_telegram(url, subs_tlg_id, f"В файле {full_cnt} {tail}.")
    else:
        logger.warning("Unable to send telegram message. Empty telegram_id..")


def make_zones_list(zones, regions, ecoregions):
    """Make list of zones as list from string."""
    zone_list = ''
    if (zones is not None) and (zones != ''):
        zone_list = zones
    elif (regions is not None) and (regions != ''):
        zone_list = get_zone_ids_for_region(regions.split(","))
    elif (ecoregions is not None) and (ecoregions != ''):
        zone_list = get_zone_ids_for_ecoregion(ecoregions.split(","))
    return zone_list


def filter_zones(cursor, zonelist, critical, zones_tab):
    """Filter non-critical zones from list."""
    if (critical is not None) and critical > 0:
        cursor.execute(f"""SELECT id
                        FROM {zones_tab}
                        WHERE
                                category = 'торфяник'
                                AND critical >= {critical}
                        """)
        relevant_zones = cursor.fetchall()
        # logger.debug(f"Relevant zones: {relevant_zones}")
        zone_list = zonelist.split(",")
        logger.debug(f"Zonelist: {zone_list}")
        cutted_zonelist = ""
        for rec in relevant_zones:
            # logger.debug(f"Rec: {rec}")
            # logger.debug(f"Rec[0]: {str(rec[0])}")
            if str(rec[0]) in zone_list:
                cutted_zonelist += f"'{rec[0]}',"
        cutted_zonelist = cutted_zonelist[0:-1]
        logger.debug(f"Cutted zonelist: {cutted_zonelist}")
    else:
        cutted_zonelist = zonelist
    return cutted_zonelist


def send_to_subscribers_job():
    """Send to subscribers job main function."""
    logger.info("---------------------------------")
    logger.info("Process [send_engine.py] started.")

    currtime = time.localtime()
    date = time.strftime("%Y-%m-%d", currtime)
    now_hour = time.strftime("%H", currtime)

    [year_tab, subs_tab, zones_tab, buffers_tab] = get_config("tables",
                                                              ["year_tab",
                                                               "subs_tab",
                                                               "oopt_zones",
                                                               "oopt_buffers"])
    [data_root, temp_folder] = get_config("path", ["data_root",
                                                   "temp_folder"])
    [to_dir] = get_config("yadisk", ["yadisk_out_path"])
    [url] = get_config("telegramm", ["url"])
    [outline] = get_config("tables", ["vip_zones"])
    [alerts_check_time] = get_config("alerts", ["check_time"])
    [smf_check_time] = get_config("smf", ["check_time"])
    conn, cursor = get_tuple_cursor()

    # Загружаем данные о подписчиках
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
            # logger.debug(f"Zones list: {zone_list}.")
            if zone_list == '':
                logger.warning(f"Empty zones list for {subs.subs_name}.")
                continue
            if subs.zone_types == '':
                logger.warning(f"Empty zone types list for {subs.subs_name}.")
                continue
            else:
                zone_type_list = subs.zone_types.split(",")
            logger.info(f"Checking zones for zone-types in: {zone_type_list}.")
            for zone_type in zone_type_list:
                # if zone_type == "peat":
                if zone_type == "test":
                    filtered_zone_list = filter_zones(cursor,
                                                      zone_list,
                                                      subs.critical,
                                                      zones_tab)
                    if filtered_zone_list == "":
                        logger.info(f"Empty peat zones for {subs.subs_name}.")
                        continue
                else:
                    filtered_zone_list = zone_list
                logger.info(f"Check {zone_type} points now.")
                num_points, stat, extent = make_subs_table(conn,
                                                           cursor,
                                                           year_tab,
                                                           zones_tab,
                                                           buffers_tab,
                                                           filtered_zone_list,
                                                           subs.check_buffers,
                                                           subs.period,
                                                           subs.subs_id,
                                                           subs.filter_tech,
                                                           zone_type,
                                                           subs.critical)
                # num_points = len(stat)
                if (num_points > 0) and (subs.teleg_stat or subs.email_stat):
                    msg = make_zone_msg(cursor, zones_tab, subs.critical,
                                        stat, extent, zone_type)
                    if subs.teleg_stat:
                        logger.info("Sending stat to telegram...")
                        send_to_telegram(url, subs.tlg_id, msg)
                    if subs.email_stat:
                        logger.info("Sending stat to email...")
                        subject = f"Statistic per last {subs.period}"
                        maillist = subs.emails.replace(" ", "").split(",")
                        send_email_message(maillist, subject, msg)
                else:
                    logger.info("Zero-point stat. Don`t sending.")
                if (subs.email_point or subs.teleg_point):
                    if (num_points > 0) or subs.send_empty:
                        dst_file = make_subs_kml(subs.period,
                                                 subs.subs_name,
                                                 subs.subs_id,
                                                 result_dir,
                                                 date,
                                                 int(now_hour),
                                                 subs.critical)
                        if subs.email_point:
                            send_email_to_subs(subs.emails,
                                               subs.period,
                                               date,
                                               num_points,
                                               dst_file)
                        if subs.teleg_point:
                            send_tlg_to_subs(subs.tlg_id,
                                             dst_file,
                                             url,
                                             num_points)
                        drop_temp_file(dst_file)
                    else:
                        logger.info(f"Don`t send zero-point {zone_type} file.")
                drop_whom_table(conn, cursor, subs.subs_id)
            if now_hour == sendtimelist[0] and subs.ya_disk:
                logger.info("Writing to yadisk...")
                subs_folder = f"for_s{str(subs.subs_name)}"
                write_to_yadisk(dst_file_name, result_dir, to_dir, subs_folder)
            # drop_whom_table(conn, cursor, subs.subs_id)
        else:
            logger.info("Do anything? It`s not time yet!")

    if now_hour == alerts_check_time:
        [alerts_period] = get_config("alerts", ["period"])
        new_alerts(alerts_period, date)

    close_conn(conn, cursor)
    logger.info("Process [send_engine.py] stopped.")


if __name__ == "__main__":
    send_to_subscribers_job()
