#-------------------------------------------------------------------------------
# Name:        fa_service
# Purpose:
# Author:      Chaus
# Created:     28.10.2020
#-------------------------------------------------------------------------------import os

from configparser import ConfigParser
from falogging import log
import os, sys, re, posixpath
import requests
import psycopg2
from psycopg2.extras import NamedTupleCursor
import yadisk
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


#Получение параметров из узла "node" ini-файла "inifile"
#Список имен параметров передается в "param_names"
#Возвращаем список значений
def get_db_config():
    inifile = "firealert.ini"
    node = "db"
    param_names = ["dbserver","dbport","dbname", "dbuser", "dbpass"]
    base_path = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_path, inifile)

    # get the config
    if os.path.exists(config_path):
        cfg = ConfigParser()
        cfg.read(config_path)
    else:
        log("Ini-file %s not found!.." %(inifile))
        sys.exit(1)

    # extract params
    param = [cfg.get(node, param_name) for param_name in param_names]
    return param

#Получение параметров из таблицы 'parameters' БД
#Префикс параметра передается в "node"
#Список имен параметров передается в "param_names"
#Возвращаем список значений
def get_config(node, param_names):
    [dbserver,dbport,dbname,dbuser,dbpass] = get_db_config()
    param_table = 'parameters'
    conn = psycopg2.connect(host=dbserver, port=dbport, dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()

    if type(param_names) is not list:
        param_names = [param_names]

    val_list = [None]*len(param_names)
    i = 0
    for param_name in param_names:
        key = '%(n)s.%(p)s' %{'n':node, 'p':param_name}

        try:
            cursor.execute("SELECT strval FROM %(t)s WHERE key = '%(k)s'" %{'t':param_table,'k':key})
            result = cursor.fetchone()
            strval = result[0]
            cursor.execute("SELECT intval FROM %(t)s WHERE key = '%(k)s'" %{'t':param_table,'k':key})
            result = cursor.fetchone()
            intval = result[0]
            cursor.execute("SELECT lstval FROM %(t)s WHERE key = '%(k)s'" %{'t':param_table,'k':key})
            result = cursor.fetchone()
            lststr = result[0]
            if lststr != None:
                lstval = str_to_lst(lststr)
            else:
                lstval = None

            log('Parameter readed:%s'%(key))
        except IOError as e:
            log('Error getting statistic for region:$s'%e)

        if strval != None:
            val_list[i] = strval
        elif lstval != None:
            val_list[i] = lstval
        elif intval != None:
            val_list[i] = intval
        else:
            val_list[i] = None
        
        i+=1

        if type(param_names) is not list:
            val_list = val_list[0]

    cursor.close
    conn.close

    return val_list

def get_path(root_path,folder):
    log("Creating folder %s..." %folder)
    base_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(base_path, root_path)
    result_path = os.path.join(result_path, folder)
    if not os.path.exists(result_path):
        try:
            os.mkdir(result_path)
            log("Created %s" %result_path)
        except OSError:
            log("Unable to create %s" %result_path)
    return result_path

def str_to_lst(param_str):
    param_lst = re.sub(r'[\'\"]\s*,\s*[\'\"]','\',\'', param_str.strip('\'\"[]')).split("\',\'")
    return param_lst

#Создание геоиндексов для таблиц из списка outlines
def index_all_region(conn,cursor,outlines):
    log("Creating indexes for %s..." %outlines)
    [dbserver,dbport,dbname,dbuser,dbpass] = get_db_config()

    conn = psycopg2.connect(host=dbserver, port=dbport, dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()

    for outline in outlines:
        try:
            cursor.execute('CREATE INDEX %s_idx ON %s USING GIST (geog)'%(outline,outline))
            conn.commit()
        except IOError as e:
            log('Error indexing geometry $s' % e)

    cursor.close
    conn.close

def get_cursor():
    [dbserver,dbport,dbname,dbuser,dbpass] = get_db_config()
    conn = psycopg2.connect(host=dbserver, port=dbport, dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor()
    return conn, cursor

def get_tuple_cursor():
    [dbserver,dbport,dbname,dbuser,dbpass] = get_db_config()
    conn = psycopg2.connect(host=dbserver, port=dbport, dbname=dbname, user=dbuser, password=dbpass)
    cursor = conn.cursor(cursor_factory=NamedTupleCursor)
    return conn, cursor

def close_conn(conn, cursor):
    conn.commit()
    cursor.close
    conn.close

def send_to_telegram(url, chat, text):
    params = {'chat_id': chat, 'text': text}
    response = requests.post(url + 'sendMessage', data=params)
    if response.status_code != 200:
        #raise Exception("post_text error: %s" %response.status_code)
        log("post_text error: %s" %response.status_code)
        log("Text:\n<<%s>>" %text)
    return response

def send_doc_to_telegram(url, chat, file):
    post_data = {'chat_id': chat}
    post_file = {'document': file}
    response = requests.post(url + 'sendDocument', data=post_data, files = post_file)
    if response.status_code != 200:
        #raise Exception("post_text error: %s" %response.status_code)
        log("post_text error: %s" %response.status_code)
    return response

def send_img_to_telegram(url, chat, file):
    post_data = {'chat_id': chat}
    post_file = {'photo': file}
    response = requests.post(url + 'sendPhoto', data=post_data, files = post_file)
    if response.status_code != 200:
        #raise Exception("post_text error: %s" %response.status_code)
        log("post_text error: %s" %response.status_code)
    return response

# Сохраняем созданную таблицу в kml-файл для последующей отправки подписчикам
def write_to_kml(dst_file,whom):

    [dbserver,dbport,dbname,dbuser,dbpass] = get_db_config()

    subs_tab = 'for_s%s' %str(whom)
    log("Writting data from %(s)s table to kml-file: %(f)s..." %{'s':subs_tab, 'f':dst_file})
    if os.path.isfile(dst_file):
        os.remove(dst_file)
        log('Owerwrite kml %s...'%(dst_file))
    else:
        log('Create new kml %s...'%(dst_file))
    command = """ogr2ogr -f "KML" %(d)s PG:"host=%(h)s user=%(u)s dbname=%(b)s password=%(w)s port=%(p)s" %(s)s"""%{'d':dst_file,'s':subs_tab,'h':dbserver,'u':dbuser,'b':dbname,'w':dbpass,'p':dbport}
    os.system(command)
    log('Done.')

#Запись файла "file" из каталога "from_dir" на я-диск в каталог "to_dir" подкаталог "subscriber"
def write_to_yadisk(file, from_dir, to_dir, whom):
    log("Writing file %s to Yandex disk..." %file)
    [yadisk_token] = get_config("yadisk", ["yadisk_token"])
    y = yadisk.YaDisk(token=yadisk_token)
    to_dir = to_dir + whom
    p = from_dir.split(from_dir)[1].strip(os.path.sep)
    dir_path = posixpath.join(to_dir, p)
    if not y.exists(dir_path):
        try:
            y.mkdir(dir_path)
            log('Path created on yadisk %s.'%(dir_path))
        except yadisk.exceptions.PathExistsError:
            log('Path cannot be created %s.'%(dir_path))
    file_path = posixpath.join(dir_path, file)
    p_sys = p.replace("/", os.path.sep)
    in_path = os.path.join(from_dir, p_sys, file)
    try:
        y.upload(in_path, file_path, overwrite = True)
        log('Written to yadisk %s'%(file_path))
    except yadisk.exceptions.PathExistsError:
        log('Path not exist %s.'%(dir_path))
        pass

def attach_file(msg, filepath):                                 # Функция по добавлению конкретного файла к сообщению
    try:
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
    except IOError:
        msg = "Error opening attachment file %s" % filepath
        log(msg)
        sys.exit(1)

def process_attachement(msg, files):                        # Функция по обработке списка, добавляемых к сообщению файлов
    for f in files:
        if os.path.isfile(f):                               # Если файл существует
            attach_file(msg,f)                              # Добавляем файл к сообщению
        elif os.path.exists(f):                             # Если путь не файл и существует, значит - папка
            dir = os.listdir(f)                             # Получаем список файлов в папке
            for file in dir:                                # Перебираем все файлы и...
                attach_file(msg,f+"/"+file)                 # ...добавляем каждый файл к сообщению

#Send an email with an attachment
def send_email_with_attachment(maillist, subject, body_text, filelist):
    log("Sending e-mail to addresses: %s..." %maillist)

    # extract server and from_addr from config
    [host,from_addr,user,pwd] = get_config("smtp", ["server", "from_addr", "user", "pwd"])

    # create the message
    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)

    if body_text:
        msg.attach( MIMEText(body_text) )

    msg["To"] = ', '.join(maillist)

    process_attachement(msg, filelist)

    mailserver = smtplib.SMTP(host,587)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.ehlo()
    mailserver.login(user, pwd)
    mailserver.sendmail(from_addr, maillist, msg.as_string())
    mailserver.quit()
    log('Mail sended.')

def send_email_message(maillist, subject, body_text):
    log("Sending e-mail to addresses: %s..." %maillist)

    # extract server and from_addr from config
    [host,from_addr,user,pwd] = get_config("smtp", ["server", "from_addr", "user", "pwd"])

    # create the message
    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)

    if body_text:
        msg.attach( MIMEText(body_text) )

    msg["To"] = ', '.join(maillist)

    mailserver = smtplib.SMTP(host,587)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.ehlo()
    mailserver.login(user, pwd)
    mailserver.sendmail(from_addr, maillist, msg.as_string())
    mailserver.quit()
    log('Mail sended.')


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

def smf_new_topic(smf_url, smf_user, smf_pass, board, subject, msg, icon="xx", notify=0, lock=0, sticky=0):
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

def points_tail(nump):
    str_nump = str(nump)
    if str_nump[-1] == '1':
        tail = 'точка'
    elif str_nump[-1] in ['2','3','4']:
        tail = 'точки'
    else:
        tail = 'точек'
    return tail
