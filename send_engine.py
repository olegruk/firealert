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
    make_tlg_peat_stat_msg,
    get_zone_ids_for_region,
    get_zone_ids_for_fed_distr,
    new_alerts)
from mylogger import init_logger

logger = init_logger(loglevel="Debug")


def count_crit_points(cursor, subs_tab, limit):
    """Check count of critical points for zonelist."""
    # logger.info("Getting statistic for %s..."%(reg))
    if limit is not None:
        stat = (
            f"""
            SELECT count(*)
            FROM {subs_tab}
            WHERE critical >= {limit}
            """
        )
        try:
            cursor.execute(stat)
            critical_cnt = cursor.fetchone()[0]
        except psycopg2.Error as err:
            logger.error(f"Error getting critical statistic for zone: {err}")
            critical_cnt = 0
    else:
        logger.warning("Critical limit is Null. Count of points setted to 0.")
        critical_cnt = 0
    return critical_cnt


def make_subs_table(conn, cursor, year_tab, zone_type, id_list,
                    ch_buf, period, whom, filter_tech, st_lim, dl_lim):
    """Create table with points for subscriber with ID 'subs_id'."""
    logger.info(f"Creating table for subs_id: {whom}...")
    subs_tab = f"for_s{whom}"
    zones_tab = zone_type + "_zones"
    marker = f"[s{whom}]"
    period = f"{period} hours"

    if zone_type == 'full':
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
                    critical,
                    geog
                FROM {year_tab}
                WHERE
                    date_time >= TIMESTAMP 'now' - INTERVAL '{period}'
                    AND (
                        whom is Null
                        OR POSITION('{marker}' in whom) = 0
                    )
                    AND NOT (
                        {filter_tech}
                        AND (tech_id IS NOT NULL)
                    )
            """,
            f"""
            UPDATE {year_tab}
                SET
                    whom = whom || '{marker}'
                WHERE
                    date_time > TIMESTAMP 'now' - INTERVAL '{period}'
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
                    AND whom is Null
                    AND NOT (
                        {filter_tech}
                        AND (tech_id IS NOT NULL)
                    )
            """,
            f"""
            ALTER TABLE {subs_tab}
                ADD COLUMN description VARCHAR(500),
                ADD COLUMN name VARCHAR(100) DEFAULT ''
            """,
            f"""
            UPDATE {subs_tab}
                SET
                    description =
                        'Дата: ' || date || '\n' ||
                        'Время: ' || time || '\n' ||
                        'Широта: ' || latitude || '\n' ||
                        'Долгота (ID): ' || longitude || '\n' ||
                        'Регион: ' || region
            """
        )

        stat_buf = stat_non_buf
    else:
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
                    {zone_type}_id as zone_id,
                    {zone_type}_dist as dist,
                    critical,
                    geog
                FROM {year_tab}
                WHERE
                    date_time >= TIMESTAMP 'now' - INTERVAL '{period}'
                    AND {zone_type}_id IN ({id_list})
                    AND {zone_type}_dist = 0
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
                ADD COLUMN {zone_type}_name VARCHAR(100)
            """,
            f"""
            UPDATE {subs_tab}
                SET
                    {zone_type}_name = {zones_tab}.name
                FROM {zones_tab}
                WHERE
                    {subs_tab}.zone_id = {zones_tab}.id
            """,
            f"""
            UPDATE {subs_tab}
                SET
                    description =
                        'Дата: ' || date || '\n' ||
                        'Время: ' || time || '\n' ||
                        'Широта: ' || latitude || '\n' ||
                        'Долгота: ' || longitude || '\n' ||
                        'Регион: ' || region || '\n' ||
                        'Территория: ' || {zone_type}_name
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
                    {zone_type}_id as zone_id,
                    {zone_type}_dist as dist,
                    critical,
                    {zone_type}_dist AS distance,
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
                ADD COLUMN {zone_type}_name VARCHAR(100)
            """,
            f"""
            UPDATE {subs_tab}
                SET
                    {zone_type}_name = {zones_tab}.name
                FROM {zones_tab}
                WHERE
                    {subs_tab}.zone_id = {zones_tab}.id
            """,
            f"""
            UPDATE {subs_tab}
                SET
                    description =
                        'Дата: ' || date || '\n' ||
                        'Время: ' || time || '\n' ||
                        'Широта: ' || latitude || '\n' ||
                        'Долгота: ' || longitude || '\n' ||
                        'Регион: ' || region || '\n' ||
                        'Территория: ' || {zone_type}_name
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
    if zone_type != "full":
        cursor.execute(f"""SELECT
                                {subs_tab}.zone_id,
                                {subs_tab}.region,
                                {subs_tab}.{zone_type}_name,
                                count(*),
                                ST_XMin(ST_Extent({zones_tab}.geog::geometry))
                                    AS x_min,
                                ST_YMin(ST_Extent({zones_tab}.geog::geometry))
                                    AS y_min,
                                ST_XMax(ST_Extent({zones_tab}.geog::geometry))
                                    AS x_max,
                                ST_YMax(ST_Extent({zones_tab}.geog::geometry))
                                    AS y_max,
                                sp_count_firms_in_zone('{subs_tab}',
                                                        {subs_tab}.zone_id)
                                    AS zone_count,
                                sp_count_firms_in_buffer('{subs_tab}',
                                                        {subs_tab}.zone_id)
                                    AS buffer_count
                            FROM {subs_tab} 
                                 LEFT JOIN {zones_tab} 
                                 ON {subs_tab}.zone_id = {zones_tab}.id
                            GROUP BY {subs_tab}.zone_id,
                                     {subs_tab}.region,
                                     {subs_tab}.{zone_type}_name""")
    else:
        cursor.execute(f"""SELECT 
                                0 AS full_id,
                                region,
                                '' AS zone_name,
                                count(*),
                                0 AS x_min,
                                0 AS y_min,
                                0 AS x_max,
                                0 AS y_max,
                                count(*) AS zone_count,
                                0 AS buffer_count
                            FROM {subs_tab}
                            GROUP BY region""")

    res_tab = cursor.fetchall()
    cursor.execute(f"""SELECT
                            ST_XMax(ST_Extent(geog::geometry)) AS x_max,
                            ST_YMax(ST_Extent(geog::geometry)) AS y_max,
                            ST_XMin(ST_Extent(geog::geometry)) AS x_min,
                            ST_YMin(ST_Extent(geog::geometry)) AS y_min,
                            ST_Extent(geog::geometry) AS subs_extent
                       FROM {subs_tab}""")
    points_extent = cursor.fetchone()
    num = 0
    for str in res_tab:
        num += str[3]
    stat_cr_count = count_crit_points(cursor, subs_tab, st_lim)
    dnld_cr_count = count_crit_points(cursor, subs_tab, dl_lim)
    return num, stat_cr_count, dnld_cr_count, res_tab, points_extent, subs_tab


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


def make_zone_msg(crit_count, stat, extent, zone_type):
    """Generate a statistic message for zones to sending over telegram."""
    if zone_type == "oopt":
        intro = "ООПТ"
    elif zone_type == "peat":
        intro = "торфяниках"
    elif zone_type == "attn":
        intro = "зонах особого контроля"
    elif zone_type == "full":
        intro = "регионе мониторинга"
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
        if (st_str[4] != st_str[6]) and (st_str[5] != st_str[7]):
            zone_link = (f"<a href="
                         f"'https://maps.natureconservation.kz/"
                         f"portal/apps/mapviewer/"
                         f"index.html?webmap=65d8526143c3479dbd510c37ccec6f42"
                         f"&extent="
                         f"{st_str[4]},{st_str[5]},{st_str[6]},{st_str[7]}'"
                         f">{st_str[2]}</a>")
        else:
            zone_link = (f"<a href="
                         f"'https://maps.natureconservation.kz/"
                         f"portal/apps/mapviewer/"
                         f"index.html?webmap=65d8526143c3479dbd510c37ccec6f42"
                         f"&extent="
                         f"{st_str[4]},{st_str[5]},{st_str[6]},{st_str[7]}"
                         f"&level=9'"
                         f">{st_str[2]}</a>")
        # zone_link = st_str[2]
        msg += (f"\r\n{st_str[1]} - {zone_link}: "
                f"{st_str[3]} ({st_str[8]}, {st_str[9]})")
        full_cnt += st_str[3]
    msg += f"\nВсего точек: {full_cnt}"
    if crit_count > 0:
        msg += f"\nВысокой опасности: {crit_count}"
    x_max = extent[0]
    y_max = extent[1]
    x_min = extent[2]
    y_min = extent[3]
    if (x_max != x_min) and (y_max != y_min):
        res_link = (f"\n\n"
                    f"<a href="
                    f"'https://maps.natureconservation.kz/"
                    f"portal/apps/mapviewer/"
                    f"index.html?webmap=65d8526143c3479dbd510c37ccec6f42"
                    f"&extent={x_min},{y_min},{x_max},{y_max}'"
                    f">Посмотреть на карте...</a>")
    else:
        res_link = (f"\n\n"
                    f"<a href="
                    f"'https://maps.natureconservation.kz/"
                    f"portal/apps/mapviewer/"
                    f"index.html?webmap=65d8526143c3479dbd510c37ccec6f42"
                    f"&extent={x_min},{y_min},{x_max},{y_max}&level=9'"
                    f">Посмотреть на карте...</a>")
    # res_link = ''
    msg += res_link
    return msg


def make_subs_kml(point_period,
                  subs_name,
                  subs_id,
                  result_dir,
                  date,
                  int_now_hour,
                  limit):
    """Write kml-file for subscriber."""
    logger.info("Creating kml file...")
    dst_file_name = make_file_name(point_period,
                                   date,
                                   subs_name,
                                   result_dir,
                                   int_now_hour)
    dst_file = os.path.join(result_dir, dst_file_name)
    write_to_kml(dst_file, subs_id, limit)
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


def make_zones_list(zones_tab, zones, regions, fed_distr):
    """Make list of zones as list from string."""
    zone_list = ''
    if (zones is not None) and (zones != ''):
        zone_list = zones
    elif (regions is not None) and (regions != ''):
        zone_list = get_zone_ids_for_region(zones_tab, regions.split(","))
    elif (fed_distr is not None) and (fed_distr != ''):
        zone_list = get_zone_ids_for_fed_distr(zones_tab, fed_distr.split(","))
    return zone_list


def filter_zones(cursor, zonelist, limit, zones_tab):
    """Filter non-critical zones from list."""
    if (limit is not None) and limit > 0:
        cursor.execute(f"""SELECT id
                                FROM {zones_tab}
                                WHERE
                                    critical >= {limit}
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

    [year_tab, subs_tab] = get_config("tables", ["year_tab", "subs_tab"])
    [data_root, temp_folder] = get_config("path", ["data_root", "temp_folder"])
    [to_dir] = get_config("yadisk", ["yadisk_out_path"])
    [url] = get_config("telegramm", ["url"])
    [outline] = get_config("tables", ["vip_zones"])
    [alerts_check_time] = get_config("alerts", ["check_time"])
    # [smf_check_time] = get_config("smf", ["check_time"])
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
            if subs.zone_types == '' or subs.zone_types is None:
                logger.warning(f"Empty zone types list for {subs.subs_name}.")
                continue
            else:
                zone_type_list = subs.zone_types.split(",")
            logger.info(f"Checking zones for zone-types in: {zone_type_list}.")
            for zone_type in zone_type_list:
                if zone_type == 'peat':
                     zones = subs.peat_zones
                elif zone_type == 'oopt':
                    zones = subs.oopt_zones
                elif zone_type == 'attn':
                    zones = subs.attn_zones
                else:
                    zones = ''
                zones_tab = zone_type + "_zones"
                if zone_type == 'full':
                    zone_list = ''
                else:
                    zone_list = make_zones_list(zones_tab,
                                                zones,
                                                subs.regions,
                                                subs.fed_reg)
                logger.debug(f"Zones list: {zone_list}.")
                if zone_list == '' and zone_type != 'full':
                    logger.warning(f"Empty zones list for {subs.subs_name}.")
                    continue
                if ((zone_type == "test") 
                        and (subs.dnld_lim in [2,4,8,16,32,64])):
                    filtered_zone_list = filter_zones(cursor,
                                                      zone_list,
                                                      subs.dnld_lim,
                                                      zones_tab)
                    if filtered_zone_list == "":
                        logger.info(f"Empty peat zones for {subs.subs_name}.")
                        continue
                else:
                    filtered_zone_list = zone_list
                logger.info(f"Check {zone_type} points now.")
                num_p, st_cr, dl_cr, stat, extent, s_tab = make_subs_table(conn,
                                                        cursor,
                                                        year_tab,
                                                        zone_type,
                                                        filtered_zone_list,
                                                        subs.check_buffers,
                                                        subs.period,
                                                        subs.subs_id,
                                                        subs.filter_tech,
                                                        subs.stat_lim,
                                                        subs.dnld_lim)
                if (num_p > 0) and (subs.teleg_stat or subs.email_stat):
                    msg = make_zone_msg(st_cr, stat, extent, zone_type)
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
                    if (dl_cr > 0) or subs.send_empty:
                        dst_file = make_subs_kml(subs.period,
                                                 subs.subs_name,
                                                 subs.subs_id,
                                                 result_dir,
                                                 date,
                                                 int(now_hour),
                                                 subs.dnld_lim)
                        if subs.email_point:
                            send_email_to_subs(subs.emails,
                                               subs.period,
                                               date,
                                               dl_cr,
                                               dst_file)
                        if subs.teleg_point:
                            send_tlg_to_subs(subs.tlg_id,
                                             dst_file,
                                             url,
                                             dl_cr)
                        drop_temp_file(dst_file)
                    else:
                        logger.info(f"Don`t send zero-point {zone_type} file.")
                drop_whom_table(conn, cursor, subs.subs_id)
            if now_hour == sendtimelist[0] and subs.ya_disk:
                logger.info("Writing to yadisk...")
                subs_folder = f"for_s{str(subs.subs_name)}"
                dst_file_name = make_file_name(subs.point_period,
                                               date,
                                               subs.subs_name,
                                               result_dir,
                                               0)
                write_to_yadisk(dst_file_name, result_dir, to_dir, subs_folder)
            # drop_whom_table(conn, cursor, subs.subs_id)
            if (now_hour == sendtimelist[0]
                    and (subs.teleg_digest or subs.email_digest)
                    and (subs.regions is not None)
                    and (subs.regions != '')
                    and ('peat' in zone_type_list)):
                reg_list = subs.regions.split(",")
                msg = make_tlg_peat_stat_msg(reg_list,
                                             subs.stat_period,
                                             subs.stat_lim)
                if subs.teleg_digest:
                    logger.info("Sending digest to telegram...")
                    send_to_telegram(url, subs.tlg_id, msg)
                if subs.email_digest:
                    logger.info("Sending digest to email...")
                    subject = f"Digest per last {subs.stat_period}"
                    maillist = subs.email.replace(" ", "").split(",")
                    send_email_message(maillist, subject, msg)
        else:
            logger.info("Do anything? It`s not time yet!")

    if now_hour == alerts_check_time:
        [alerts_period] = get_config("alerts", ["period"])
        new_alerts(alerts_period, date)

    close_conn(conn, cursor)
    logger.info("Process [send_engine.py] stopped.")


if __name__ == "__main__":
    send_to_subscribers_job()
