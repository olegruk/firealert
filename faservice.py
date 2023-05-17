"""
Module of firealert robot.

Contains a lot of service functions.

Created:     28.10.2020

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
import sys
import re
import posixpath
import requests
import psycopg2
import yadisk
import smtplib
from configparser import ConfigParser
from psycopg2.extras import NamedTupleCursor

# Добавляем необходимые подклассы - MIME-типы
import mimetypes  # Импорт класса MIME-типов, базирующихся на расширении файла
from email import encoders  # Импортируем энкодер
from email.utils import formatdate
from email.mime.base import MIMEBase  # Общий тип
from email.mime.text import MIMEText  # Текст/HTML
from email.mime.image import MIMEImage  # Изображения
from email.mime.audio import MIMEAudio  # Аудио
from email.mime.multipart import MIMEMultipart  # Многокомпонентный объект
from mylogger import get_logger

logger = get_logger()


def get_db_config():
    """Get parameters from ini-file node."""
    inifile = "firealert.ini"
    node = "db"
    param_names = ["dbserver", "dbport", "dbname", "dbuser", "dbpass"]
    base_path = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_path, inifile)

    # get the config
    if os.path.exists(config_path):
        cfg = ConfigParser()
        cfg.read(config_path)
    else:
        logger.info(f"Ini-file {inifile} not found!..")
        sys.exit(1)

    # extract params
    param = [cfg.get(node, param_name) for param_name in param_names]
    return param


def get_config(node, param_names):
    """Get parameters from 'parameters' table."""
    [dbserver, dbport, dbname, dbuser, dbpass] = get_db_config()
    param_table = "parameters"
    conn = psycopg2.connect(host=dbserver,
                            port=dbport,
                            dbname=dbname,
                            user=dbuser,
                            password=dbpass)
    cursor = conn.cursor()

    if type(param_names) is not list:
        param_names = [param_names]

    val_list = [None]*len(param_names)
    i = 0
    for param_name in param_names:
        key = f"{node}.{param_name}"

        try:
            cursor.execute(
                f"SELECT strval FROM {param_table} WHERE key = '{key}'")
            result = cursor.fetchone()
            strval = result[0]
            cursor.execute(
                f"SELECT intval FROM {param_table} WHERE key = '{key}'")
            result = cursor.fetchone()
            intval = result[0]
            cursor.execute(
                f"SELECT lstval FROM {param_table} WHERE key = '{key}'")
            result = cursor.fetchone()
            lststr = result[0]
            if lststr is not None:
                lstval = str_to_lst(lststr)
            else:
                lstval = None

            logger.info(f"Parameter readed: {key}")
        except IOError as err:
            logger.error(f"Error getting statistic for region: {err}")

        if strval is not None:
            val_list[i] = strval
        elif lstval is not None:
            val_list[i] = lstval
        elif intval is not None:
            val_list[i] = intval
        else:
            val_list[i] = None

        i += 1

        if type(param_names) is not list:
            val_list = val_list[0]

    cursor.close
    conn.close

    return val_list


def get_path(root_path, folder):
    """Create a new folder in a root path."""
    logger.info(f"Creating folder {folder}...")
    base_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(base_path, root_path)
    result_path = os.path.join(result_path, folder)
    if not os.path.exists(result_path):
        try:
            os.mkdir(result_path)
            logger.info(f"Created {result_path}.")
        except OSError:
            logger.error(f"Unable to create {result_path}.")
    return result_path


def str_to_lst(param_str):
    """Convert a comma-separated string to list."""
    param_lst = re.sub(
        r'[\'\"]\s*,\s*[\'\"]',
        '\',\'',
        param_str.strip('\'\"[]')).split("\',\'")
    return param_lst


def get_cursor():
    """Get a cursor for operations with db."""
    [dbserver, dbport, dbname, dbuser, dbpass] = get_db_config()
    conn = psycopg2.connect(host=dbserver,
                            port=dbport,
                            dbname=dbname,
                            user=dbuser,
                            password=dbpass)
    cursor = conn.cursor()
    return conn, cursor


def get_tuple_cursor():
    """Get a tuple cursor for operations with db."""
    [dbserver, dbport, dbname, dbuser, dbpass] = get_db_config()
    conn = psycopg2.connect(host=dbserver,
                            port=dbport,
                            dbname=dbname,
                            user=dbuser,
                            password=dbpass)
    cursor = conn.cursor(cursor_factory=NamedTupleCursor)
    return conn, cursor


def close_conn(conn, cursor):
    """Close db connection."""
    conn.commit()
    cursor.close
    conn.close


def send_to_telegram(url, chat, text):
    """Send a simple message to telegram."""
    params = {"chat_id": chat, "text": text, "parse_mode": 'HTML'}
    response = requests.post(url + "sendMessage", data=params)
    if response.status_code != 200:
        logger.error(f"Post_text error: {response.status_code}")
        logger.error(f"Text:\n<<{text}>>")
        # raise Exception(f"post_text error: {response.status_code}")
    return response


def send_doc_to_telegram(url, chat, file):
    """Send a doc-file to telegram."""
    post_data = {'chat_id': chat}
    post_file = {'document': file}
    response = requests.post(url + 'sendDocument',
                             data=post_data,
                             files=post_file)
    if response.status_code != 200:
        # raise Exception(f"post_text error: {response.status_code}")
        logger.error(f"post_text error: {response.status_code}")
    return response


def send_img_to_telegram(url, chat, file):
    """Send an image file to telegram."""
    post_data = {'chat_id': chat}
    post_file = {'photo': file}
    response = requests.post(url + "sendPhoto",
                             data=post_data,
                             files=post_file)
    if response.status_code != 200:
        # raise Exception(f"post_text error: {response.status_code}")
        logger.warning(f"Post_text error: {response.status_code}.")
    return response


def write_to_kml(dst_file, whom):
    """Save a table to kml-file."""
    [dbserver, dbport, dbname, dbuser, dbpass] = get_db_config()

    subs_tab = f"for_s{whom}"
    logger.info(f"Writting data from {subs_tab} table "
                f"to kml-file {dst_file}...")
    if os.path.isfile(dst_file):
        os.remove(dst_file)
        logger.info(f"Owerwrite kml {dst_file}...")
    else:
        logger.info(f"Create new kml {dst_file}...")
    command = f"""ogr2ogr \
                    -f "KML" \
                    {dst_file} \
                    PG:"host={dbserver} \
                        user={dbuser} \
                        dbname={dbname} \
                        password={dbpass} \
                        port={dbport}" \
                    {subs_tab}"""
    os.system(command)
    logger.info('Done.')


def write_to_yadisk(file, from_dir, to_dir, whom):
    """Write file from "from_dir" to yadisk into "to_dir:subscriber" folder."""
    logger.info(f"Writing file {file} to Yandex disk...")
    [yadisk_token] = get_config("yadisk", ["yadisk_token"])
    y = yadisk.YaDisk(token=yadisk_token)
    # is_valid_token = y.check_token()
    # logger.info('Result of token validation: %s.'%(is_valid_token))
    to_dir = to_dir + whom
    # logger.info(f"to_dir: {to_dir}.")
    # logger.info(f"from_dir: {from_dir}.")
    p = from_dir.split(from_dir)[1].strip(os.path.sep)
    # logger.info(f"p: {p}.")
    dir_path = posixpath.join(to_dir, p)
    # logger.info(f"dir_path: {dir_path}.")
    if not y.exists(dir_path):
        try:
            y.mkdir(dir_path)
            logger.info(f"Path created on yadisk {dir_path}.")
        except yadisk.exceptions.PathExistsError:
            logger.error(f"Path cannot be created {dir_path}.")
    file_path = posixpath.join(dir_path, file)
    # logger.info(f"file_path: {file_path}.")
    p_sys = p.replace("/", os.path.sep)
    in_path = os.path.join(from_dir, p_sys, file)
    # logger.info(f"in_path: {in_path}.")
    try:
        # y.upload(in_path, file_path, overwrite=True)
        y.upload(in_path, file_path, overwrite=True, timeout=(20.0, 100.0))
        logger.info(f"Written to yadisk {file_path}")
    except yadisk.exceptions.YaDiskError as err:
        logger.error(f"Error at file uploading: {err}.")
        pass


def remove_folder_from_yadisk(a_dir):
    """Clear folder "a_dir" on Yandex disk from any files."""
    logger.info(f"Removing Yandex disk folder {a_dir}...")
    [yadisk_token] = get_config("yadisk", ["yadisk_token"])
    y = yadisk.YaDisk(token=yadisk_token)
    if y.exists(a_dir):
        try:
            y.remove(a_dir, permanently=True)
            logger.info(f"Yandex disk directoy {a_dir} removed.")
        except yadisk.exceptions.YaDiskError as err:
            logger.error(f"Error at dir clearing: {err}.")
            pass
    else:
        logger.info(f"Yandex disk directoy {a_dir} not exist.")

def attach_file(msg, filepath):
    """Attach a file to email message."""
    try:
        filename = os.path.basename(filepath)  # Get a file name
        ctype, encoding = mimetypes.guess_type(filepath)  # Check file type
        if ctype is None or encoding is not None:  # If not detected...
            ctype = "application/octet-stream"  # Use common type
        maintype, subtype = ctype.split('/', 1)  # Get type and subtype
        if maintype == "text":  # If a text file
            with open(filepath) as fp:  # Open to read
                file = MIMEText(fp.read(), _subtype=subtype)  # Use MIME type
                fp.close()  # Close a file after use!!!
        elif maintype == "image":  # If an image
            with open(filepath, "rb") as fp:
                file = MIMEImage(fp.read(), _subtype=subtype)
                fp.close()
        elif maintype == "audio":  # If an audio
            with open(filepath, "rb") as fp:
                file = MIMEAudio(fp.read(), _subtype=subtype)
                fp.close()
        else:  # Unknown file type
            with open(filepath, "rb") as fp:
                file = MIMEBase(maintype, subtype)  # Use common MIME
                file.set_payload(fp.read())  # Common type content (payload)
                fp.close()
                encoders.encode_base64(file)
        file.add_header("Content-Disposition", "attachment", filename=filename)
        msg.attach(file)
    except IOError:
        msg = f"Error opening attachment file {filepath}"
        logger.error(msg)
        sys.exit(1)


def process_attachement(msg, files):
    """Process a list of added files."""
    for f in files:
        if os.path.isfile(f):  # If a file..
            attach_file(msg, f)  # add it to message
        elif os.path.exists(f):  # If not a file? then folder
            dir = os.listdir(f)  # Get a list of files in folder
            for file in dir:
                attach_file(msg, f"{f}/{file}")


def send_email_with_attachment(maillist, subject, body_text, filelist):
    """Send an email with an attachment."""
    logger.info(f"Sending e-mail to addresses: {maillist}...")

    # extract server and from_addr from config
    [host, from_addr, user, pwd] = get_config("smtp",
                                              ["server",
                                               "from_addr",
                                               "user",
                                               "pwd"])

    # create the message
    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)

    if body_text:
        msg.attach(MIMEText(body_text))

    msg["To"] = ', '.join(maillist)

    process_attachement(msg, filelist)

    mailserver = smtplib.SMTP(host, 587)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.ehlo()
    mailserver.login(user, pwd)
    mailserver.sendmail(from_addr, maillist, msg.as_string())
    mailserver.quit()
    logger.info("Mail sended.")


def send_email_message(maillist, subject, body_text):
    """Send email common function."""
    logger.info(f"Sending e-mail to addresses: {maillist}...")

    # extract server and from_addr from config
    [host, from_addr, user, pwd] = get_config("smtp",
                                              ["server",
                                               "from_addr",
                                               "user",
                                               "pwd"])

    # create the message
    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)

    if body_text:
        msg.attach(MIMEText(body_text))

    msg["To"] = ', '.join(maillist)

    mailserver = smtplib.SMTP(host, 587)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.ehlo()
    mailserver.login(user, pwd)
    mailserver.sendmail(from_addr, maillist, msg.as_string())
    mailserver.quit()
    logger.info("Mail sended.")


def points_tail(nump):
    """Make a word endins for numeral."""
    str_nump = str(nump)
    if str_nump[-1] == "1":
        tail = "точка"
    elif str_nump[-1] in ["2", "3", "4"] and nump > 10 and nump < 20:
        tail = "точек"
    elif str_nump[-1] in ["2", "3", "4"]:
        tail = "точки"
    else:
        tail = "точек"
    return tail
