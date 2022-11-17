# -*- coding: utf-8 -*-

import os, datetime, re
import imaplib, email, base64
from email.header import decode_header
from falogging import log, start_logging, stop_logging
from faservice import get_config, get_path, get_cursor, close_conn, send_to_telegram, send_img_to_telegram

#codepage='iso-8859-1'
#codepage='windows-1251'
#codepage='unicode-escape'
#, errors='ignore'

def parse_multipart(email_message, result_dir, uid, codepage):
    result = ''
    if email_message.is_multipart():
        for part in email_message.get_payload():
            ctype = part.get_content_type()
            if ctype in ['image/jpeg', 'image/png']:
                old_file_name = ''.join((t[0].decode(codepage)) for t in decode_header(part.get_filename()))
                dst_file_name = '{uid}-{oldname}'.format(uid=uid, oldname=old_file_name)
                dst_file = os.path.join(result_dir,dst_file_name)
                open(dst_file, 'wb').write(part.get_payload(decode=True))
                #result.append([ctype,dst_file_name])
            elif ctype in ['text/plain']:#, 'text/html']:
                #body = part.get_payload(decode=True)
                body = part.get_payload(decode=True).decode(codepage, errors='ignore')
                result = result + body + '\n'
            elif ctype in ['multipart/related', 'multipart/alternative', 'multipart/mixed']:
                result = result + parse_multipart(part, result_dir, uid, codepage) + '\n'
    else:
        ctype = email_message.get_payload().get_content_type()
        result = email_message.get_payload(decode=True).decode(codepage, errors='ignore')
        #body = email_message.get_payload(decode=True).decode(part.get_content_charset())
    return result

def get_last_uid(conn, cursor, angel_tab):
    cursor.execute("SELECT max(uid) FROM %s"%angel_tab)
    last_uid = cursor.fetchone()[0]
    return last_uid

def store_message(conn, cursor, angel_tab, peat_tab, uid, date, time, description, region, district, place, lat, lon, azimuth, google, yandex, send_to):
    #print('Uid: {uid}'.format(uid=uid))
    stat = """
        INSERT INTO %(a)s (uid,date,time,description,region,district,place,lat,lon,azimuth,google,yandex,send_to,geom)
            VALUES ('%(u)s','%(d)s','%(t)s','%(i)s','%(r)s','%(c)s','%(p)s','%(l)s','%(o)s','%(z)s','%(g)s','%(y)s','%(n)s',ST_GeomFromText('POINT(%(o)s %(l)s)',4326))
    """%{'a':angel_tab,'u':uid,'d':date,'t':time,'i':description,'r':region,'c':district,'p':place,'l':lat,'o':lon,'z':azimuth,'g':google,'y':yandex,'n':send_to}
    check = """
        UPDATE %(a)s SET
            peat_id = %(p)s.unique_id,
            burn = %(p)s.burn_indx
        FROM %(p)s
        WHERE %(a)s.uid = '%(u)s' AND ST_Intersects(st_transform(%(a)s.geom::geometry, 3857), %(p)s.geom)
    """%{'a':angel_tab, 'p':peat_tab, 'u':uid}
    try:
        cursor.execute(stat)
        conn.commit()
        log("A message #%s added."%uid)
    except IOError as e:
        log('Error adding message:$s'%e)
    try:
        cursor.execute(check)
        conn.commit()
        log("A peatlands check for #%s done."%uid)
    except IOError as e:
        log('Error adding message:$s'%e)
    cursor.execute("SELECT peat_id, burn FROM %(a)s WHERE %(a)s.uid = '%(u)s'"%{'a':angel_tab, 'u':uid})
    return cursor.fetchone()#[0]
        
def telegram_images(url, chat_id, f):
    if os.path.exists(f):
        dir = os.listdir(f)
        for file in dir:
            dst_file = os.path.join(f, file)
            photo = open(dst_file, 'rb')
            send_img_to_telegram(url, chat_id, photo)
            os.remove(dst_file)

def drop_temp_files(result_dir):
    for the_file in os.listdir(result_dir):
        file_path = os.path.join(result_dir, the_file)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            log('Cannot remove files:$s' %e)

def angel_mail_job():
    start_logging('angel_mail.py')
    [angel_tab, peat_tab] = get_config("tables", ['angel_tab', 'angelpeat_tab'])
    [url] = get_config("telegramm", ["url"])
    [imap_server,imap_port,codepage,imap_login,imap_password,chat_list,lim] = get_config("angel", ["imap_server","imap_port","imap_codepage","imap_login","imap_password","chat_list","lim"])
    [data_root,angel_folder] = get_config("path", ["data_root", "angel_folder"])

    #currtime = time.localtime()
    #date = time.strftime('%Y-%m-%d',currtime)
    #now_hour = time.strftime('%H',currtime)
    yesterday = (datetime.date.today() - datetime.timedelta(1)).strftime("%d-%b-%Y")
    today = datetime.date.today().strftime("%Y-%m-%d")
    #Создаем каталог для записи временных файлов
    result_dir = get_path(data_root,angel_folder)

    conn, cursor = get_cursor()

    mail = imaplib.IMAP4_SSL(imap_server, imap_port)
    mail.login(imap_login, imap_password)

    #mail.select("Inbox")
    #mail.select("Sent")
    mail.select("&BB4EQgQ,BEAEMAQyBDsENQQ9BD0ESwQ1-") #Так звучит "Отправленные" на mail.ru

    result, data = mail.uid('search', None, "ALL") # Выполняет поиск и возвращает UID писем.
    #result, data = mail.uid('search', None, '(HEADER Subject "My Search Term")')
    #result, data = mail.uid('search', None, '(HEADER Received "localhost")')
    #result, data = mail.uid('search', None, '(SENTSINCE "19-Apr-2021")'.format(date=yesterday))
    #result, data = mail.uid('search', None, '(SENTSINCE {date} HEADER Subject "My Subject" NOT FROM "yuji@grovemade.com")'.format(date=yesterday))
    max_uid = get_last_uid(conn, cursor, angel_tab)
    #max_uid = 5500
    #print(max_uid)
    uid_list = []
    for uid in data[0].split():
        if int(uid) > max_uid:
            uid_list.append(uid)

    #print(uid_list)

    for uid in uid_list:
        result, fetch_data = mail.uid('fetch', uid, '(RFC822)')
        #result, fetch_data = mail.uid('fetch', uid, '(BODY[HEADER.FIELDS (DATE SUBJECT)]])')
        raw_email = fetch_data[0][1]
        raw_email_string = raw_email.decode()
        email_message = email.message_from_string(raw_email_string)
        dig_uid = int(uid)
        send_to = email_message['To']
        if not(send_to):
            send_to = email_message['Cc']
        if send_to[0:7] == '=?UTF-8':
            send_to = ''.join((t[0].decode(codepage)) for t in decode_header(send_to))
        #log('Debug!!! Send to is:\n%s'%send_to)
        #date_time = email_message['Date']
        from_whom = email_message['From']
        #log('Debug!!! From is:\n%s'%from_whom)
        if from_whom[0:7] == '=?UTF-8?':
            from_whom = ''.join((t[0].decode(codepage)) for t in decode_header(from_whom))
        subj = email_message['Subject']
        #log('Debug!!! Subj is:\n%s'%subj)
        #parse_subj = email.utils.parseaddr(email_message['Subject'])
        #log('Debug!!! Parse_subj is:\n%s'%parse_subj[0])
        #log('Debug!!! Parse_subj is:\n%s'%parse_subj[1])
        #log('Debug!!! ---------------------------------------------------------------------')
        if subj:
            subj = ''.join((t[0].decode(codepage)) for t in decode_header(subj))
        else:
            subj = ''
            #subj = ''.join((t[0].decode(codepage)) for t in decode_header(parse_subj[0],parse_subj[1]))
        #attr_from = [''.join((t[0].decode()) for t in decode_header(email.utils.parseaddr(email_message['From'])[0])), email.utils.parseaddr(email_message['From'])[1]]
        #attr_from = [''.join((t[0]) for t in decode_header(email.utils.parseaddr(email_message['From'])[0])), email.utils.parseaddr(email_message['From'])[1]]
        #attr_id = email_message['Message-Id']
        #message = parse_multipart(email_message,result_dir,dig_uid,codepage)
        #if not(re.search(r'Fwd:', subj)) and re.search(r'Оперативный дежурный', message):
        if not(re.search(r'Fwd:', subj)) and not(re.search(r'Re:', subj)) and re.search(r'\d{1,2}/\d{1,2}/\d{2,4}\s+\d{1,2}:\d\d\s+UTC', subj):
            message = parse_multipart(email_message,result_dir,dig_uid,codepage)
            date_time = re.search(r'Дата и время:.*\n', message)
            if date_time:
                date_time = date_time[0]
                dmy = re.search(r'\d{1,2}/\d{1,2}/\d{2,4}', date_time)
                if dmy:
                    dmy = dmy[0]
                    date = '{yyyy}-{mm}-{dd}'.format(yyyy=dmy[6:10],mm=dmy[3:5],dd=dmy[0:2])
                else:
                    date = today
                time = re.search(r'\d{1,2}:\d\d', date_time)
                if time:
                    time = time[0]
                else:
                    time = '00:00'
            else:
                date = today
                time = '00:00'
            description = re.search(r'Описание наблюдаемого ЧС:.*\n', message)
            if description:
                description = description[0]
                description = re.sub(r'Описание наблюдаемого ЧС:\s*','',description)
                description = re.sub(r'\s*\n','',description)
            else:
                description = ''
            region = re.search(r'Область:.*\n', message)
            if region:
                region = region[0]
                region = re.sub(r'Область:\s*','',region)
                region = re.sub(r'\s*\n','',region)
            else:
                region = ''
            district = re.search(r'Район:.*\n', message)
            if district:
                district = district[0]
                district = re.sub(r'Район:\s*','',district)
                district = re.sub(r'\s*\n','',district)
            else:
                district = ''
            place = re.search(r'Ближайший населенный пункт:.*\n', message)
            if place:
                place = place[0]
                place = re.sub(r'Ближайший населенный пункт:\s*','',place)
                place = re.sub(r'\s*\n','',place)
            else:
                place = ''
            latlon = re.search(r'N\d{1,2}\.\d{1,8} E\d{1,3}\.\d{1,8}', message)
            if latlon:
                latlon = latlon[0]
                lat = re.search(r'N\d{1,2}\.\d{1,8}', latlon)
                if lat:
                    lat = lat[0][1:]
                else:
                    lat = ''
                lon = re.search(r'E\d{1,3}\.\d{1,8}', latlon)
                if lon:
                    lon = lon[0][1:]
                else:
                    lon = ''
            else:
                lat = ''
                lon = ''
            azimuth = re.search(r'Курс: .*\n', message)
            if azimuth:
                azimuth = azimuth[0]
                azimuth = re.sub(r'Курс:\s*','',azimuth)
                azimuth = re.sub(r'\s*\n','',azimuth)
            #azimuth = re.search(r'Курс: \d{1,3}\.\d{1,2}', message)[0]#[6:]
            else:
                azimuth = 'не определен'
            google = re.search(r'Google Maps:.*\n', message)
            if google:
                google = google[0]
                google = re.sub(r'Google Maps:\s*','',google)
                google = re.sub(r'\s*\n','',google)
            else:
                google = ''
            yandex = re.search(r'Yandex Maps:.*\n', message)
            if yandex:
                yandex = yandex[0]
                yandex = re.sub(r'Yandex Maps:\s*','',yandex)
                yandex = re.sub(r'\s*\n','',yandex)            
            else:
                yandex = ''
            if lat != '' and lon != '':
                is_peat = store_message(conn, cursor, angel_tab, peat_tab, dig_uid, date, time, description, region, district, place, lat, lon, azimuth, google, yandex, send_to)
                if is_peat[0]:
                    burn = int(is_peat[1])
                    if  burn >= int(lim):
                        telegram_mes = "Сообщение #{uid}\n{date} {time} UTC\nЧС: {desc}\nТорфяник: ID{peat}, горимость - {burn}\nОбласть: {reg}\nРайон: {dist}\nН.п.: {place}\nN{lat} E{lon}\nАзимут: {az}\nGoogle Maps: {google}\nYandex Maps: {yandex}".format(uid=dig_uid,date=date,time=time,desc=description,peat=is_peat[0],burn=burn,reg=region,dist=district,place=place,lat=lat,lon=lon,az=azimuth,google=google,yandex=yandex)
                        #print(telegram_mes)
                        for chat_id in chat_list:
                            send_to_telegram(url, chat_id, telegram_mes)
                            telegram_images(url, chat_id, result_dir)
    drop_temp_files(result_dir)
    mail.close()
    mail.logout()
    close_conn(conn, cursor)

    stop_logging('angel_mail.py')

#main
if __name__ == "__main__":
    angel_mail_job()