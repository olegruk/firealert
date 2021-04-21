# -*- coding: utf-8 -*-

import imaplib, email, base64
from email.header import decode_header
import os, time, re
import datetime
from falogging import log, start_logging, stop_logging
from faservice import get_config, get_path, get_cursor, close_conn, send_to_telegram, send_doc_to_telegram

from config import imap_login, imap_password

[angel_tab] = get_config("tables", ['angel_tab'])
[url, chat_id] = get_config("telegramm", ["url", "tst_chat_id"])

imap_server = "imap.mail.ru"
imap_port = "993"
codepage='utf-8'
#codepage='iso-8859-1'
#codepage='windows-1251'
#codepage='unicode-escape'
#, errors='ignore'

def parse_multipart(email_message, result_dir, uid):
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
                result = result + parse_multipart(part, result_dir, uid) + '\n'
    else:
        ctype = email_message.get_payload().get_content_type()
        result = email_message.get_payload(decode=True).decode(codepage, errors='ignore')
        #body = email_message.get_payload(decode=True).decode(part.get_content_charset())
    return result

def get_last_uid(conn, cursor):
    cursor.execute("SELECT max(uid) FROM %s"%angel_tab)
    last_uid = cursor.fetchone()[0]
    return last_uid

def store_message(conn, cursor, uid, date, time, description, region, district, place, lat, lon, azimuth, google, yandex, send_to):
    print('Uid: {uid}'.format(uid=uid))
    stat = """
        INSERT INTO %(a)s (uid,date,time,description,region,district,place,lat,lon,azimuth,google,yandex,send_to,geom)
            VALUES ('%(u)s','%(d)s','%(t)s','%(i)s','%(r)s','%(c)s','%(p)s','%(l)s','%(o)s','%(z)s','%(g)s','%(y)s','%(n)s',ST_GeomFromText('POINT(%(o)s %(l)s)',4326))
    """%{'a':angel_tab,'u':uid,'d':date,'t':time,'i':description,'r':region,'c':district,'p':place,'l':lat,'o':lon,'z':azimuth,'g':google,'y':yandex,'n':send_to}
    try:
        cursor.execute(stat)
        conn.commit()
        log("A message #%s added."%uid)
    except IOError as e:
        log('Error adding message:$s'%e)
        
def process_attachement(msg, files):                        # Функция по обработке списка, добавляемых к сообщению файлов
    for f in files:
        if os.path.isfile(f):                               # Если файл существует
            attach_file(msg,f)                              # Добавляем файл к сообщению
        elif os.path.exists(f):                             # Если путь не файл и существует, значит - папка
            dir = os.listdir(f)                             # Получаем список файлов в папке
            for file in dir:                                # Перебираем все файлы и...
                attach_file(msg,f+"/"+file)                 # ...добавляем каждый файл к сообщению

def angel_mail_job():
    start_logging('angel_mail.py')
    #currtime = time.localtime()
    #date = time.strftime('%Y-%m-%d',currtime)
    #now_hour = time.strftime('%H',currtime)
    yesterday = (datetime.date.today() - datetime.timedelta(1)).strftime("%d-%b-%Y")
    #Создаем каталог для записи временных файлов
    [data_root,angel_folder] = get_config("path", ["data_root", "angel_folder"])
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
    max_uid = 5500
    #max_uid = get_last_uid(conn, cursor)
    #print(max_uid)
    uid_list = []
    for uid in data[0].split():
        if int(uid) > max_uid:
            uid_list.append(uid)

    print(uid_list)

    for uid in uid_list:
        result, fetch_data = mail.uid('fetch', uid, '(RFC822)')
        #result, fetch_data = mail.uid('fetch', uid, '(BODY[HEADER.FIELDS (DATE SUBJECT)]])')
        raw_email = fetch_data[0][1]
        raw_email_string = raw_email.decode()
        email_message = email.message_from_string(raw_email_string)

        dig_uid = int(uid)
        send_to = email_message['To']
        #date_time = email_message['Date']
        subj = ''.join((t[0].decode(codepage)) for t in decode_header(email_message['Subject']))
        #subj = email_message['Subject']
        #attr_from = [''.join((t[0].decode()) for t in decode_header(email.utils.parseaddr(email_message['From'])[0])), email.utils.parseaddr(email_message['From'])[1]]
        #attr_from = [''.join((t[0]) for t in decode_header(email.utils.parseaddr(email_message['From'])[0])), email.utils.parseaddr(email_message['From'])[1]]
        #attr_id = email_message['Message-Id']
        #message = parse_multipart(email_message,result_dir,dig_uid)
        #if not(re.search(r'Fwd:', subj)) and re.search(r'Оперативный дежурный', message):
        if not(re.search(r'Fwd:', subj)) and re.search(r'\d\d/\d\d/\d\d\d\d \d\d:\d\d UTC', subj):
            message = parse_multipart(email_message,result_dir,dig_uid)
            date_time = re.search(r'Дата и время:.*\n', message)[0]#[14:-1]
            dmy = re.search(r'\d\d/\d\d/\d\d\d\d', date_time)[0]
            date = '{yyyy}-{mm}-{dd}'.format(yyyy=dmy[6:10],mm=dmy[3:5],dd=dmy[0:2])
            time = re.search(r'\d\d:\d\d', date_time)[0]
            description = re.search(r'Описание наблюдаемого ЧС:.*\n', message)[0]#[26:-1]
            description = re.sub(r'Описание наблюдаемого ЧС:\s*','',description)
            description = re.sub(r'\s*\n','',description)
            region = re.search(r'Область:.*\n', message)[0]#[9:-1]
            region = re.sub(r'Область:\s*','',region)
            region = re.sub(r'\s*\n','',region)
            district = re.search(r'Район:.*\n', message)[0]#[7:-1]
            district = re.sub(r'Район:\s*','',district)
            district = re.sub(r'\s*\n','',district)
            place = re.search(r'Ближайший населенный пункт:.*\n', message)[0]#[28:-1]
            place = re.sub(r'Ближайший населенный пункт:\s*','',place)
            place = re.sub(r'\s*\n','',place)
            latlon = re.search(r'N\d{1,2}\.\d{1,8} E\d{1,3}\.\d{1,8}', message)[0]
            lat = re.search(r'N\d{1,2}\.\d{1,8}', latlon)[0][1:]
            lon = re.search(r'E\d{1,3}\.\d{1,8}', latlon)[0][1:]
            azimuth = re.search(r'Курс: .*\n', message)[0]#[6:]
            azimuth = re.sub(r'Курс:\s*','',azimuth)
            azimuth = re.sub(r'\s*\n','',azimuth)
            #azimuth = re.search(r'Курс: \d{1,3}\.\d{1,2}', message)[0]#[6:]
            google = re.search(r'Google Maps:.*\n', message)[0]#[13:-1]
            google = re.sub(r'Google Maps:\s*','',google)
            google = re.sub(r'\s*\n','',google)
            yandex = re.search(r'Yandex Maps:.*\n', message)[0]#[13:-1]
            yandex = re.sub(r'Yandex Maps:\s*','',yandex)
            yandex = re.sub(r'\s*\n','',yandex)            
            store_message(conn, cursor, dig_uid, date, time, description, region, district, place, lat, lon, azimuth, google, yandex, send_to)
            telegram_mes = "Сообщение #{uid}\n{date} {time} UTC\nЧС: {desc}\nОбласть: {reg}\nРайон: {dist}\nН.п.: {place}\nN{lat} E{lon}\nАзимут: {az}\nGoogle Maps: {google}\nYandex Maps: {yandex}".format(uid=dig_uid,date=date,time=time,desc=description,reg=region,dist=district,place=place,lat=lat,lon=lon,az=azimuth,google=google,yandex=yandex)
            send_to_telegram(url, chat_id, telegram_mes)


    mail.close()
    mail.logout()
    close_conn(conn, cursor)

    stop_logging('angel_mail.py')

#main
if __name__ == "__main__":
    angel_mail_job()