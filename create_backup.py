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

import os
import yadisk
import posixpath

from faservice import get_config, get_path
from falogging import start_logging, stop_logging, log

def create_today_backup(dbname, dbuser, dst_folder, dst_file):
    dst_file = os.path.join(dst_folder, dst_file)
    if os.path.isfile(dst_file):
        os.remove(dst_file)
        log('Owerwrite backup %s...'%(dst_file))
    else:
        log('Create new backup %s...'%(dst_file))
    command = "pg_dump -U %(u)s -w %(d)s > %(f)s"%{'u': dbuser, 'd': dbname, 'f': dst_file}
    os.system(command)
    log('Done.')

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
        log('Written to yadisk %s'%(file_path))
    except yadisk.exceptions.PathExistsError:
        log('Path not exist %s.'%(dir_path))
        pass

def create_backup_job():

    start_logging('create_backup.py')

     # extract db params from config
    [dbname,dbuser] = get_config("db", ["dbname", "dbuser"])
    [backup_folder] = get_config("path", ["backup_folder"])
    [to_dir,yadisk_token] = get_config("yadisk", ["yadisk_bckup_path", "yadisk_token"])

    dst_path = get_path('', backup_folder)
    dst_file = '%s.dump'%(dbname)
    create_today_backup(dbname, dbuser, dst_path, dst_file)
    write_to_yadisk(dst_file, dst_path, to_dir, '', yadisk_token)

    stop_logging('create_backup.py')

#main
if __name__ == "__main__":
    create_backup_job()
