# -*- coding: utf-8 -*-

import imaplib, email, base64
from email.header import decode_header
import os, time
from falogging import log, start_logging, stop_logging
from faservice import get_config, get_path

from config import imap_login, imap_password

imap_server = "imap.mail.ru"
imap_port = "993"
codepage='utf-8'
#codepage='iso-8859-1'
#codepage='windows-1251'
#codepage='unicode-escape'
#, errors='ignore'

def print_list(lst):
    for [ctype, body] in lst:
        if type(body) is str:
            print(ctype,body)
        elif type(body) is list:
            print_list(body)

def parse_multipart(email_message, result_dir):
    result = []
    if email_message.is_multipart():
        for part in email_message.get_payload():
            ctype = part.get_content_type()
            if ctype in ['image/jpeg', 'image/png']:
                dst_file_name = ''.join((t[0].decode(codepage)) for t in decode_header(part.get_filename()))
                dst_file = os.path.join(result_dir,dst_file_name)
                open(dst_file, 'wb').write(part.get_payload(decode=True))
                result.append([ctype,dst_file_name])
            elif ctype in ['text/plain']:#, 'text/html']:
                #body = part.get_payload(decode=True)
                body = part.get_payload(decode=True).decode(codepage, errors='ignore')
                result.append([ctype,body])
            elif ctype in ['multipart/related', 'multipart/alternative', 'multipart/mixed']:
                result.append([ctype,parse_multipart(part, result_dir)])
    else:
        ctype = email_message.get_payload().get_content_type()
        body = email_message.get_payload(decode=True).decode(codepage, errors='ignore')
        #body = email_message.get_payload(decode=True).decode(part.get_content_charset())
        result = [[ctype,body]]
    return result

def angel_mail_job():

    start_logging('angel_mail.py')

    currtime = time.localtime()
    date = time.strftime('%Y-%m-%d',currtime)
    now_hour = time.strftime('%H',currtime)

    #Создаем каталог для записи временных файлов
    [data_root,temp_folder] = get_config("path", ["data_root", "temp_folder"])
    result_dir = get_path(data_root,temp_folder)


    mail = imaplib.IMAP4_SSL(imap_server, imap_port)
    mail.login(imap_login, imap_password)

    #mail.select("Inbox")
    #mail.select("Sent")
    mail.select("&BB4EQgQ,BEAEMAQyBDsENQQ9BD0ESwQ1-")


    result, data = mail.uid('search', None, "ALL") # Выполняет поиск и возвращает UID писем.
    latest_email_uid = data[0].split()[-1]
    result, data = mail.uid('fetch', latest_email_uid, '(RFC822)')
    raw_email = data[0][1]
    raw_email_string = raw_email.decode()

    email_message = email.message_from_string(raw_email_string)

    attr_to = email_message['To']
    attr_from = [''.join((t[0].decode()) for t in decode_header(email.utils.parseaddr(email_message['From'])[0])), email.utils.parseaddr(email_message['From'])[1]]
    #attr_from = [''.join((t[0]) for t in decode_header(email.utils.parseaddr(email_message['From'])[0])), email.utils.parseaddr(email_message['From'])[1]]
    attr_date = email_message['Date']
    attr_subj = ''.join((t[0].decode(codepage)) for t in decode_header(email_message['Subject']))
    #attr_subj = email_message['Subject']
    attr_id = email_message['Message-Id']
    attr_body = ''

    print('To: %s'%attr_to)
    print('From: %s <%s>'%(attr_from[0],attr_from[1]))
    print('Date: %s'%attr_date)
    print('Subject: %s'%attr_subj)
    print('Message-Id: %s'%attr_id)
    print('Body:')
    body = parse_multipart(email_message,result_dir)
    print_list(body)

    mail.close()
    mail.logout()

    stop_logging('angel_mail.py')

#main
if __name__ == "__main__":
    angel_mail_job()