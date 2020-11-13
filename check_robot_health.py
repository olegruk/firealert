#-------------------------------------------------------------------------------
# Name:        Check_robot_health
# Purpose:
# Author:      Chaus
# Created:     24.01.2020
#-------------------------------------------------------------------------------

import os, time
import smtplib
# Добавляем необходимые подклассы - MIME-типы
import mimetypes                                            # Импорт класса для обработки неизвестных MIME-типов, базирующихся на расширении файла
from email import encoders                                  # Импортируем энкодер
from email.utils import formatdate
from email.mime.base import MIMEBase                        # Общий тип
from email.mime.text import MIMEText                        # Текст/HTML
from email.mime.image import MIMEImage                      # Изображения
from email.mime.audio import MIMEAudio                      # Аудио
from email.mime.multipart import MIMEMultipart              # Многокомпонентный объект
from falogging import log, start_logging, stop_logging
from faservice import get_config, send_to_telegram

def is_files_exist(filelist):
    [log_folder] = get_config("path", ["log_folder"])
    base_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(base_path, log_folder)
    existlist = []
    existcount = 0
    for onefile in filelist:
        logfile = os.path.join(result_path, onefile)
        if os.path.isfile(logfile) and os.path.getsize(logfile) > 0:
            existlist.append(logfile)
            existcount += 1
    return existcount, existlist

def process_attachement(msg, files):                        # Функция по обработке списка, добавляемых к сообщению файлов
    for f in files:
        if os.path.isfile(f):                               # Если файл существует
            attach_file(msg,f)                              # Добавляем файл к сообщению
        elif os.path.exists(f):                             # Если путь не файл и существует, значит - папка
            dir = os.listdir(f)                             # Получаем список файлов в папке
            for file in dir:                                # Перебираем все файлы и...
                attach_file(msg,f+"/"+file)                 # ...добавляем каждый файл к сообщению

def attach_file(msg, filepath):                             # Функция по добавлению конкретного файла к сообщению
    filename = os.path.basename(filepath)                   # Получаем только имя файла
    ctype, encoding = mimetypes.guess_type(filepath)        # Определяем тип файла на основе его расширения
    if ctype is None or encoding is not None:               # Если тип файла не определяется
        ctype = 'application/octet-stream'                  # Будем использовать общий тип
    maintype, subtype = ctype.split('/', 1)                 # Получаем тип и подтип
    if maintype == 'text':                                  # Если текстовый файл
        with open(filepath) as fp:                          # Открываем файл для чтения
            file = MIMEText(fp.read(), _subtype=subtype)    # Используем тип MIMEText
            fp.close()                                      # После использования файл обязательно нужно закрыть
    elif maintype == 'image':                               # Если изображение
        with open(filepath, 'rb') as fp:
            file = MIMEImage(fp.read(), _subtype=subtype)
            fp.close()
    elif maintype == 'audio':                               # Если аудио
        with open(filepath, 'rb') as fp:
            file = MIMEAudio(fp.read(), _subtype=subtype)
            fp.close()
    else:                                                   # Неизвестный тип файла
        with open(filepath, 'rb') as fp:
            file = MIMEBase(maintype, subtype)              # Используем общий MIME-тип
            file.set_payload(fp.read())                     # Добавляем содержимое общего типа (полезную нагрузку)
            fp.close()
            encoders.encode_base64(file)                    # Содержимое должно кодироваться как Base64
    file.add_header('Content-Disposition', 'attachment', filename=filename) # Добавляем заголовки
    msg.attach(file)                                        # Присоединяем файл к сообщению

#Send an email with an attachment
def send_email_with_errlogs(emails, logfiles):

    # extract server and from_addr from config
    [host,from_addr,user,pwd] = get_config("smtp", ["server", "from_addr", "user", "pwd"])

    #header = 'Content-Disposition', 'attachment; filename="%s"' % file_to_attach
    subject = "Detected log files with errors"
    body_text = "In the attachment log files with errors.\r\nCheck and correct current robot algorithms."

    # create the message
    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)

    if body_text:
        msg.attach( MIMEText(body_text) )

    msg["To"] = ', '.join(emails)

    process_attachement(msg, logfiles)

    mailserver = smtplib.SMTP(host,587)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.ehlo()
    mailserver.login(user, pwd)
    mailserver.sendmail(from_addr, emails, msg.as_string())
    mailserver.quit()

def rm_files(existlist):
    for f in existlist:
        if os.path.isfile(f):
            os.remove(f)

def check_robot_health_job():

    start_logging('check_robot_health_job.py')

    [url, chat_id] = get_config("telegramm", ["url", "tst_chat_id"])
    [filelist, mail_addr] = get_config("health", ["filelist", "emails"])

    count, existlist = is_files_exist(filelist)
    currtime = time.localtime()

    if count == 0 and currtime.tm_hour == 9:
        msg = 'В Багдаде все спокойно...'
        send_to_telegram(url, chat_id, msg)
    elif count == 1:
        msg = 'Здоровье подорвано! Ошибки в 1 журнале.'
        send_to_telegram(url, chat_id, msg)
    elif count > 1:
        msg = 'Держаться нету больше сил!.. Ошибки в ' + str(count) + ' журналах.'
        send_to_telegram(url, chat_id, msg)

    if count > 0:
        send_email_with_errlogs(mail_addr, existlist)
        rm_files(existlist)

    stop_logging('check_robot_health_job.py')

#main
if __name__ == "__main__":
    check_robot_health_job()
