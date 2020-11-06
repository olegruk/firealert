#-------------------------------------------------------------------------------
# Name:        Check_robot_health
# Purpose:
# Author:      Chaus
# Created:     24.01.2020
#-------------------------------------------------------------------------------


import os, time, sys
import requests
import logging
import posixpath
import smtplib
from configparser import ConfigParser
# Добавляем необходимые подклассы - MIME-типы
import mimetypes                                            # Импорт класса для обработки неизвестных MIME-типов, базирующихся на расширении файла
from email import encoders                                  # Импортируем энкодер
from email.utils import formatdate
from email.mime.base import MIMEBase                        # Общий тип
from email.mime.text import MIMEText                        # Текст/HTML
from email.mime.image import MIMEImage                      # Изображения
from email.mime.audio import MIMEAudio                      # Аудио
from email.mime.multipart import MIMEMultipart              # Многокомпонентный объект

inifile = "firealert.ini"
logfile = 'firealert.log'

#Получение параметров из узла "node" ini-файла "inifile"
#Список имен параметров передается в "param_names"
#Возвращаем список значений
def get_config(inifile, node, param_names):
    base_path = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_path, inifile)

    # get the config
    if os.path.exists(config_path):
        cfg = ConfigParser()
        cfg.read(config_path)
    else:
        log(logfile, "Ini-file %s not found!.." %(inifile))
        sys.exit(1)

    # extract params
    param = [cfg.get(node, param_name) for param_name in param_names]
    return param

def get_log_file(date):
    [logfile, log_folder] = get_config(inifile, "path", ["logfile", "log_folder"])
    logfile = "%(l)s_%(d)s.log"%{'l':logfile,'d':date}
    base_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(base_path, log_folder)
    if not os.path.exists(result_path):
        try:
            os.mkdir(result_path)
            log(logfile, "Created %s" % result_path)
        except OSError:
            log(logfile, "Unable to create %s" % result_path)
    result_path = os.path.join(result_path, logfile)
    return result_path

#Протоколирование
def log(logfile, msg):
    logging.basicConfig(format='%(asctime)s %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        filename=logfile)
    logging.warning(msg)
    #print(msg)

def send_to_telegram(url, chat, text):
    params = {'chat_id': chat, 'text': text}
    response = requests.post(url + 'sendMessage', data=params)
    if response.status_code != 200:
        raise Exception("post_text error: %s" %response.status_code)
    return response

def is_files_exist(filelist):
    [log_folder] = get_config(inifile, "path", ["log_folder"])
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
def send_email_with_errlogs(inifile, emails, logfiles):

    # extract server and from_addr from config
    [host,from_addr,user,pwd] = get_config(inifile, "smtp", ["server", "from_addr", "user", "pwd"])

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
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d',currtime)
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)

    logfile = get_log_file(date)
    log(logfile, '--------------------------------------------------------------------------------')
    log(logfile, 'Process [check_robot_health_job.py] started at %s'%(cdate))

    # extract params from config
    #[dbserver,dbport,dbname,dbuser,dbpass] = get_config(inifile, "db", ["dbserver","dbport","dbname", "dbuser", "dbpass"])
    #[year_tab] = get_config(inifile, "tables", ["year_tab"])
    #[data_root,result_folder] = get_config(inifile, "path", ["data_root", "result_folder"])
    #[to_dir,yadisk_token] = get_config(inifile, "yadisk", ["yadisk_out_path", "yadisk_token"])
    filelist = ['pycron_bckp.log', 'pycron_gamp.log', 'pycron_mkfc.log', 'pycron_stod.log', 'pycron_stos.log', 'pycron_wese.log', 'pycron_ckrh.log', 'pycron_refd.log', 'pycron_ckzn.log', 'pycron_ckst.log', 'pycron_seen.log','pycron_dama.log']
    mail_addr = ['chaus@firevolonter.ru']

    url = "https://api.telegram.org/bot990586097:AAHfAk360-IEPcgc7hitDSyD7pu9rzt5tbE/"
    chat_id = "-1001416479771" #@firealert-test

    count, existlist = is_files_exist(filelist)
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
        send_email_with_errlogs(inifile, mail_addr, existlist)
        rm_files(existlist)

    currtime = time.localtime()
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
    log(logfile, 'Process [check_robot_health_job.py] stopped at %s'%(cdate))

#main
if __name__ == "__main__":
    check_robot_health_job()
