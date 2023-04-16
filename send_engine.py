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


def count_crit_points(zones_tab, zone_id, limit):
    """Check count of critical points for zonelist."""
    # logger.info("Getting statistic for %s..."%(reg))
    conn, cursor = get_cursor()
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

    close_conn(conn, cursor)

    return critical_cnt


def make_subs_table(conn, cursor, year_tab, zones_tab, id_list,
                         period, whom, filter_tech, zone_type, limit):
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
    for rec in res_tab:
        crit_count = count_crit_points(zones_tab, rec[0], limit)
        rec.append(crit_count)
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
    elif zone_type == "safe":
        intro = "охранных зонах"
    elif zone_type == "oopt_buffer":
        intro = "буферных зонах ООПТ"
    elif zone_type == "peat_buffer":
        intro = "буферных зонах торфяников"
    elif zone_type == "ctrl_buffer":
        intro = "буферах зон особого контроля"
    elif zone_type == "safe_buffer":
        intro = "буферах охранных зон"
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
    if st_str[4] > 0:
        msg+= f"\nВысокой опасности: {st_str[4]}"
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


def send_email_to_subs(subs_emails, subs_point_period, date, full_cnt, dst_file):
    """Sending to subscriber a kml-file over email."""
    if (subs_emails is not None and subs_emails != ''):
        logger.info("Creating maillist...")
        maillist = subs_emails.replace(" ", "").split(",")
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


def send_tlg_to_subs(subs_tlg_id, dst_file, url, full_cnt):
    if subs_tlg_id is not None and subs_tlg_id != '':
        doc = open(dst_file, "rb")
        send_doc_to_telegram(url, subs_tlg_id, doc)
        tail = points_tail(full_cnt)
        send_to_telegram(url, subs_tlg_id, f"В файле {full_cnt} {tail}.")
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
    # emails             varchar()       список e-mail адресов подписчика
    # tlg_id         varchar(20)     список телеграмм-чатов подписчика
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
                                                zone_type,
                                                subs.critical)
                num_points = len(stat)
                if (num_points > 0) and (subs.teleg_stat or subs.email_stat):
                    msg = make_zone_msg(stat, extent, zone_type)
                    if subs.teleg_stat:
                        logger.info("Sending stat to telegram...")
                        send_to_telegram(url, subs.tlg_id, msg)
                    if subs.email_stat:
                        logger.info("Sending stat to email...")
                        subject = f"Statistic per last {subs.stat_period}"
                        maillist = subs.emails.replace(" ", "").split(",")
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
                                                    zone_type,
                                                    subs.critical)
                    num_points = len(stat)
                    if num_points > 0:
                        msg = make_zone_msg(stat, extent, f"{zone_type}_buffer")
                        send_to_telegram(url, subs.tlg_id, msg)
                    if (subs.email_point or subs.teleg_point):
                        if (num_points > 0) or subs.send_empty:
                            dst_file = make_subs_kml(subs.period,
                                                        subs.subs_name,
                                                        subs.subs_id,
                                                        result_dir,
                                                        date,
                                                        int(now_hour))
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

    close_conn(conn, cursor)
    logger.info("Process [send_engine.py] stopped.")


if __name__ == "__main__":
    send_to_subscribers_job()
