# -*- coding: utf-8 -*-

import imaplib, email, base64
from email.header import decode_header
import os, time, re
import datetime
from falogging import log, start_logging, stop_logging
from faservice import get_config, get_path, get_cursor, close_conn

from config import imap_login, imap_password

[angel_tab] = get_config("tables", ['angel_tab'])

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
                dst_file_name = ('%s '%uid).join((t[0].decode(codepage)) for t in decode_header(part.get_filename()))
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
    cursor.execute("SELECT max(dig_uid) FROM %s"%angel_tab)
    last_uid = cursor.fetchone()[0]
    return last_uid

def store_message(conn, cursor, uid, send_to, date_time, subj, message, lat, lon):
    dig_uid = int(uid)
    print('Dig_Uid: %s'%dig_uid)
    #print('Uid: %s'%uid)
    #print('To: %s'%send_to)
    #print('From: %s <%s>'%(attr_from[0],attr_from[1]))
    #print('Date: %s'%date_time)
    #print('Subject: %s'%subj)
    #print('Message-Id: %s'%attr_id)
    #print('Body:\n\n%s'%message)
    #print('lat: %s, lon: %s'%(lat,lon))
    #SET geom = ST_GeomFromText('POINT(%s %s)',4326)
    stat = """
        INSERT INTO %(a)s (uid,send_to,date_time,subj,message,geom,dig_uid)
            VALUES ('%(u)s', '%(t)s', '%(d)s', '%(s)s', '%(m)s', ST_GeomFromText('POINT(%(o)s %(l)s)',4326), %(g)s)
    """%{'a':angel_tab,'u':dig_uid,'t':send_to,'d':date_time,'s':subj,'m':message,'l':lat,'o':lon,'g':dig_uid}
    try:
        cursor.execute(stat)
        conn.commit()
        log("A message #%s added."%dig_uid)
    except IOError as e:
        log('Error adding message:$s'%e)
        

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
    max_uid = 5520
    max_uid = get_last_uid(conn, cursor)
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
        date_time = email_message['Date']
        subj = ''.join((t[0].decode(codepage)) for t in decode_header(email_message['Subject']))
        #subj = email_message['Subject']
        #attr_from = [''.join((t[0].decode()) for t in decode_header(email.utils.parseaddr(email_message['From'])[0])), email.utils.parseaddr(email_message['From'])[1]]
        #attr_from = [''.join((t[0]) for t in decode_header(email.utils.parseaddr(email_message['From'])[0])), email.utils.parseaddr(email_message['From'])[1]]
        #attr_id = email_message['Message-Id']
        message = parse_multipart(email_message,result_dir,dig_uid)
        if not(re.search(r'Fwd:', subj)) and re.search(r'Оперативный дежурный', message):
            latlon = re.search(r'N\d{1,2}\.\d{1,8} E\d{1,3}\.\d{1,8}', message)[0]
            lat = re.search(r'N\d{1,2}\.\d{1,8}', latlon)[0][1:]
            lon = re.search(r'E\d{1,3}\.\d{1,8}', latlon)[0][1:]
            store_message(conn, cursor, uid, send_to, date_time, subj, message, lat, lon)

    mail.close()
    mail.logout()
    close_conn(conn, cursor)

    stop_logging('angel_mail.py')

#main
if __name__ == "__main__":
    angel_mail_job()