#-------------------------------------------------------------------------------
# Name:        check_stat
# Purpose:
# Author:      Chaus
# Created:     06.02.2020
#-------------------------------------------------------------------------------

import time
import requests
from falogging import start_logging, stop_logging, log
from faservice import get_config, get_cursor, close_conn, send_to_telegram

def smf_login(session, smf_url, smf_user, smf_pass):
    # login method
    login_url1 = "index.php?action=login"
    login_url2 = "index.php?action=login2"
    # get auth_key and random input name
    login_page = session.get(smf_url + login_url1)
    smf_session_id = login_page.text.split("hashLoginPassword(this, '")[1].split("'")[0]
    smf_random_input = login_page.text.split("<input type=\"hidden\" name=\"hash_passwrd\" value=\"\" />"
                                                  "<input type=\"hidden\" name=\"")[1].split("\"")[0]
    # login
    payload = {
        'user': smf_user,
        'passwrd': smf_pass,
        'cookielength': -1,
        smf_random_input: smf_session_id,
    }
    response = session.post(smf_url + login_url2, data=payload)
    log("Login Response: %s" % response)
    return smf_session_id, smf_random_input

def new_topic(smf_url, smf_user, smf_pass, board, subject, msg, icon="xx", notify=0, lock=0, sticky=0):
    post_url1 = "index.php?action=post;board=" + str(board)
    post_url2 = "index.php?action=post2;start=0;board=" + str(board) + ".0"
    with requests.session() as session:
        smf_session_id, smf_random_input = smf_login(session, smf_url, smf_user, smf_pass)
        # get seqnum
        post_page = session.get(smf_url + post_url1, cookies=session.cookies)
        try:
            seqnum = post_page.text.split("<input type=\"hidden\" name=\"seqnum\" value=\"")[1].split("\"")[0]
            # post the post :)
            payload = {'topic': 0,
                       'subject': str(subject),
                       'icon': str(icon),
                       'sel_face': '',
                       'sel_size': '',
                       'sel_color': '',
                       'message': str(msg),
                       'message_mode': 0,
                       'notify': notify,
                       'lock': lock,
                       'sticky': sticky,
                       'move': 0,
                       'attachment[]': "",
                       'additional_options': 0,
                       str(smf_random_input): str(smf_session_id),
                       'seqnum': str(seqnum)}
            response = requests.post(smf_url + post_url2, data=payload, cookies=session.cookies)
            if response:
                return True
            else:
                return False
        except KeyError:
            return False

def check_peats_stat(conn, cursor, year_tab, alert_tab, reglist, period, critical, cur_date):
    log("Getting peats statistic...")
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
    #cursor.execute("SELECT count(*) FROM %s"%(subs_tab))
    #return cursor.fetchone()[0]

def new_alerts(conn, cursor, clust_view, alert_tab, period, cur_date):
    log("Adding alerts...")
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
        WHERE alert_date IS NULL
        """%{'a':alert_tab,'d':cur_date}
        )

    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log('Adding alerts finished.')
    except IOError as e:
        log('Error adding alerts:$s'%e)
    #cursor.execute("SELECT count(*) FROM %s"%(subs_tab))
    #return cursor.fetchone()[0]

def check_reg_stat(conn, cursor, year_tab, reg, period, critical):
    log("Getting statistic for %s..."%(reg))
    statements = (
        """
        SELECT count(*) FROM
            (SELECT name
            FROM %(y)s
            WHERE date_time >= NOW() - INTERVAL '%(p)s' AND (critical >= %(c)s OR revision >= %(c)s) AND region = '%(r)s') as critical_sel
        """%{'y':year_tab,'p':period,'c':critical,'r':reg},
        """
        SELECT count(*) FROM
            (SELECT name
            FROM %(y)s
            WHERE date_time >= NOW() - INTERVAL '%(p)s' AND region = '%(r)s') as all_sel
        """%{'y':year_tab,'p':period,'r':reg}
        )
    try:
        cursor.execute(statements[0])
        critical_cnt = cursor.fetchone()[0]
        cursor.execute(statements[1])
        all_cnt = cursor.fetchone()[0]
        log('Finished for:%s'%(reg))
    except IOError as e:
        log('Error getting statistic for region:$s'%e)

    return critical_cnt, all_cnt

def check_stat_job():
    start_logging('check_stat.py')
    
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d',currtime)
    fdate=time.strftime('%d-%m-%Y',currtime)

    # extract params from config
    [year_tab] = get_config("tables", ["year_tab"])
    [url, chat_id] = get_config("telegramm", ["url", "wrk_chat_id"])
    [period, critical_limit] = get_config("statistic", ["period", "critical_limit"])
    [smf_url, smf_user, smf_pass] = get_config("smf", ["smf_url", "smf_user", "smf_pass"])
    [peat_stat_period, peat_stat_critical,alert_tab,clust_view] = get_config("peats_stat", ["period", "critical_limit", "alert_tab", "cluster_view"])
    [reg_list_cr] = get_config("reglists", ["cr"])
    #reg_list_cr = ['Ярославская область','Тверская область','Смоленская область','Рязанская область','Московская область','Москва','Калужская область','Ивановская область','Владимирская область','Брянская область','Тульская область']
    #reg_list = "('Ярославская область','Тверская область','Смоленская область','Рязанская область','Московская область','Москва','Калужская область','Ивановская область','Владимирская область','Брянская область')"

    conn, cursor = get_cursor()

    #check_peats_stat(conn, cursor, year_tab, alert_tab, reg_list, peat_stat_period, peat_stat_critical, date)
    new_alerts(conn, cursor, clust_view, alert_tab, peat_stat_period, date)

    full_cnt = 0
    full_cr_cnt = 0
    msg = 'Количество точек:'
    smf_msg = 'Количество точек:\r\n\r\n[table]'
    smf_msg = smf_msg + '\r\n[tr][td][b]Регион[/b][/td][td]   [/td][td][b]Всего точек   [/b][/td][td][b]Критичных точек[/b][/td][/tr]'
    for reg in reg_list_cr:
        critical_cnt, all_cnt = check_reg_stat(conn, cursor, year_tab, reg, period, critical_limit)
        if all_cnt > 0:
            msg = msg + '\r\n%(r)s: %(a)s'%{'r':reg,'a':all_cnt}
            if critical_cnt > 0:
                msg = msg + '\r\nкритичных: %(c)s'%{'c':critical_cnt}
        smf_msg = smf_msg + '\r\n[tr][td]%(r)s[/td][td]   [/td][td][center]%(a)s[/center][/td][td][center]%(c)s[/center][/td][/tr]'%{'r':reg,'a':all_cnt, 'c':critical_cnt}
        full_cnt = full_cnt + all_cnt
        full_cr_cnt = full_cr_cnt + critical_cnt
    smf_msg = smf_msg + '\r\n[tr][td][b]Всего:[/b][/td][td]   [/td][td][center][b]%(a)s[/b][/center][/td][td][center][b]%(c)s[/b][/center][/td][/tr]'%{'a':full_cnt, 'c':full_cr_cnt}
    smf_msg = smf_msg + '\r\n[/table]'
    if full_cnt == 0:
        msg = 'Нет новых точек.'
        smf_msg = 'Нет новых точек.'

    send_to_telegram(url, chat_id, msg)
    new_topic(smf_url, smf_user, smf_pass, 13.0, fdate, smf_msg)

    close_conn(conn, cursor)

    stop_logging('check_stat.py')

#main
if __name__ == "__main__":
    check_stat_job()
