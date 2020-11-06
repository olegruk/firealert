#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      User
#
# Created:     10.04.2019
# Copyright:   (c) User 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------

#Восстановление: pg_restore -d newdb db.dump

import os, time, sys
import logging
from configparser import ConfigParser
import yadisk
import posixpath

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
        print(logfile, "ini-file not found!..")
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

def get_path(root_path,folder):
    base_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(base_path,root_path)
    result_path = os.path.join(result_path, folder)
    if not os.path.exists(result_path):
        try:
            os.mkdir(result_path)
            log (logfile, "Created %s" % result_path)
        except OSError:
            log (logfile, "Unable to create %s" % result_path)
    return result_path

def create_today_backup(dbname, dbuser, dst_folder, dst_file):
    dst_file = os.path.join(dst_folder, dst_file)
    if os.path.isfile(dst_file):
        os.remove(dst_file)
        log(logfile, 'Owerwrite backup %s...'%(dst_file))
    else:
        log(logfile, 'Create new backup %s...'%(dst_file))
    command = "pg_dump -U %(u)s -w %(d)s > %(f)s"%{'u': dbuser, 'd': dbname, 'f': dst_file}
    os.system(command)
    log(logfile, 'Done.')

#Запись файла "file" из каталога "from_dir" на я-диск в каталог "to_dir" подкаталог "subscriber"
def write_to_yadisk(file, from_dir, to_dir, subscriber, yadisk_token):
    y = yadisk.YaDisk(token=yadisk_token)
    to_dir = to_dir + subscriber
    p = from_dir.split(from_dir)[1].strip(os.path.sep)
    dir_path = posixpath.join(to_dir, p)
    file_path = posixpath.join(dir_path, file)
    p_sys = p.replace("/", os.path.sep)
    in_path = os.path.join(from_dir, p_sys, file)
    try:
        y.upload(in_path, file_path, overwrite = True)
        log(logfile, 'Written to yadisk %s'%(file_path))
    except yadisk.exceptions.PathExistsError:
        log(logfile, 'Path not exist %s.'%(dir_path))
        pass

def create_backup_job():
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d',currtime)
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)

    logfile = get_log_file(date)
    log(logfile, '--------------------------------------------------------------------------------')
    log(logfile, 'Process [create_backup.py] started at %s'%(cdate))

     # extract db params from config
    [dbname,dbuser,dbpass] = get_config(inifile, "db", ["dbname", "dbuser", "dbpass"])
    [backup_folder] = get_config(inifile, "path", ["backup_folder"])
    [to_dir,yadisk_token] = get_config(inifile, "yadisk", ["yadisk_bckup_path", "yadisk_token"])

    dst_path = get_path('', backup_folder)
    dst_file = '%s.dump'%(dbname)
    create_today_backup(dbname, dbuser, dst_path, dst_file)
    write_to_yadisk(dst_file, dst_path, to_dir, '', yadisk_token)

    currtime = time.localtime()
    cdate=time.strftime('%d-%m-%Y %H:%M:%S',currtime)
    log(logfile, 'Process [create_backup.py] stopped at %s'%(cdate))

#main
if __name__ == "__main__":
    create_backup_job()
