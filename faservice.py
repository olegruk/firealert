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
from falogging import log


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
        log(f"Ini-file {inifile} not found!..")
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

            log(f"Parameter readed: {key}")
        except IOError as err:
            log(f"Error getting statistic for region: {err}")

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
    log(f"Creating folder {folder}...")
    base_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(base_path, root_path)
    result_path = os.path.join(result_path, folder)
    if not os.path.exists(result_path):
        try:
            os.mkdir(result_path)
            log(f"Created {result_path}.")
        except OSError:
            log(f"Unable to create {result_path}.")
    return result_path


def str_to_lst(param_str):
    """Convert a comma-separated string to list."""
    param_lst = re.sub(
        r'[\'\"]\s*,\s*[\'\"]',
        '\',\'',
        param_str.strip('\'\"[]')).split("\',\'")
    return param_lst


def index_all_region(conn, cursor, outlines):
    """Create geoindexes for tables from outlines list."""
    log(f"Creating indexes for {outlines}...")
    [dbserver, dbport, dbname, dbuser, dbpass] = get_db_config()

    conn = psycopg2.connect(host=dbserver,
                            port=dbport,
                            dbname=dbname,
                            user=dbuser,
                            password=dbpass)
    cursor = conn.cursor()

    for outline in outlines:
        try:
            cursor.execute(
                f"CREATE INDEX {outline}_idx ON {outline} USING GIST (geog)")
            conn.commit()
        except IOError as err:
            log(f"Error indexing geometry: {err}")

    cursor.close
    conn.close


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
    params = {"chat_id": chat, "text": text}
    response = requests.post(url + "sendMessage", data=params)
    if response.status_code != 200:
        log(f"Post_text error: {response.status_code}")
        log(f"Text:\n<<{text}>>")
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
        log(f"post_text error: {response.status_code}")
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
        log(f"Post_text error: {response.status_code}.")
    return response


def write_to_kml(dst_file, whom):
    """Save a table to kml-file."""
    [dbserver, dbport, dbname, dbuser, dbpass] = get_db_config()

    subs_tab = f"for_s{str(whom)}"
    log(f"Writting data from {subs_tab} table to kml-file {dst_file}...")
    if os.path.isfile(dst_file):
        os.remove(dst_file)
        log(f"Owerwrite kml {dst_file}...")
    else:
        log(f"Create new kml {dst_file}...")
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
    log('Done.')


def write_to_yadisk(file, from_dir, to_dir, whom):
    """Write file from "from_dir" to yadisk into "to_dir:subscriber" folder."""
    log(f"Writing file {file} to Yandex disk...")
    [yadisk_token] = get_config("yadisk", ["yadisk_token"])
    y = yadisk.YaDisk(token=yadisk_token)
    # is_valid_token = y.check_token()
    # log('Result of token validation: %s.'%(is_valid_token))
    to_dir = to_dir + whom
    log(f"to_dir: {to_dir}.")
    log(f"from_dir: {from_dir}.")
    p = from_dir.split(from_dir)[1].strip(os.path.sep)
    log(f"p: {p}.")
    dir_path = posixpath.join(to_dir, p)
    log(f"dir_path: {dir_path}.")
    if not y.exists(dir_path):
        try:
            y.mkdir(dir_path)
            log(f"Path created on yadisk {dir_path}.")
        except yadisk.exceptions.PathExistsError:
            log(f"Path cannot be created {dir_path}.")
    file_path = posixpath.join(dir_path, file)
    log(f"file_path: {file_path}.")
    p_sys = p.replace("/", os.path.sep)
    in_path = os.path.join(from_dir, p_sys, file)
    log(f"in_path: {in_path}.")
    try:
        y.upload(in_path, file_path, overwrite=True, timeout=(20.0, 25.0))
        log(f"Written to yadisk {file_path}")
    except yadisk.exceptions.PathExistsError:
        log(f"Path not exist {dir_path}.")
        pass


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
        log(msg)
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
    log(f"Sending e-mail to addresses: {maillist}...")

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
    log("Mail sended.")


def send_email_message(maillist, subject, body_text):
    """Send email common function."""
    log(f"Sending e-mail to addresses: {maillist}...")

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
    log("Mail sended.")


def smf_login(session, smf_url, smf_user, smf_pass):
    """Login to smf forum."""
    login_url1 = "index.php?action=login"
    login_url2 = "index.php?action=login2"
    # get auth_key and random input name
    login_page = session.get(smf_url + login_url1)
    smf_session_id = login_page.text.split(
                        "hashLoginPassword(this, '")[1].split("'")[0]
    smf_random_input = login_page.text.split(
                        "<input type=\"hidden\" "
                        "name=\"hash_passwrd\" "
                        "value=\"\" />"
                        "<input type=\"hidden\" "
                        "name=\"")[1].split("\"")[0]
    # login
    payload = {
        "user": smf_user,
        "passwrd": smf_pass,
        "cookielength": -1,
        smf_random_input: smf_session_id,
    }
    response = session.post(smf_url + login_url2, data=payload)
    log(f"Login Response: {response}")
    return smf_session_id, smf_random_input


def smf_new_topic(smf_url, smf_user, smf_pass, board, subject,
                  msg, icon="xx", notify=0, lock=0, sticky=0):
    """Add a new topic to smf forum."""
    post_url1 = f"index.php?action=post;board={str(board)}"
    post_url2 = f"index.php?action=post2;start=0;board={str(board)}.0"
    with requests.session() as session:
        smf_session_id, smf_random_input = smf_login(session, smf_url,
                                                     smf_user, smf_pass)
        # get seqnum
        post_page = session.get(smf_url + post_url1, cookies=session.cookies)
        try:
            seqnum = post_page.text.split(
                        "<input type=\"hidden\" "
                        "name=\"seqnum\" "
                        "value=\"")[1].split("\"")[0]
            # post the post :)
            payload = {"topic": 0,
                       "subject": str(subject),
                       "icon": str(icon),
                       "sel_face": '',
                       "sel_size": '',
                       "sel_color": '',
                       "message": str(msg),
                       "message_mode": 0,
                       "notify": notify,
                       "lock": lock,
                       "sticky": sticky,
                       "move": 0,
                       "attachment[]": "",
                       "additional_options": 0,
                       str(smf_random_input): str(smf_session_id),
                       "seqnum": str(seqnum)}
            response = requests.post(smf_url + post_url2,
                                     data=payload,
                                     cookies=session.cookies)
            if response:
                return True
            else:
                return False
        except KeyError:
            return False


def points_tail(nump):
    """Make a word endins for numeral."""
    str_nump = str(nump)
    if str_nump[-1] == "1":
        tail = "точка"
    elif str_nump[-1] in ["2", "3", "4"]:
        tail = "точки"
    else:
        tail = "точек"
    return tail
