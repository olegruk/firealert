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
from faservice import get_config, get_path, write_to_yadisk
from falogging import start_logging, stop_logging, log

def create_today_backup(dbname, dbuser, dst_folder, dst_file):
    dst_file = os.path.join(dst_folder, dst_file)
    if os.path.isfile(dst_file):
        os.remove(dst_file)
        log('Owerwrite backup %s...'%(dst_file))
    else:
        log('Create new backup %s...'%(dst_file))
    command = "pg_dump -U %(u)s -w %(d)s | split -b 1G --filter='gzip' > %(f)s"%{'u': dbuser, 'd': dbname, 'f': dst_file}
    os.system(command)
    log('Done.')

def create_backup_job():

    start_logging('create_backup.py')

     # extract db params from config
    [dbname,dbuser] = get_config("db", ["dbname", "dbuser"])
    [backup_folder] = get_config("path", ["backup_folder"])
    [to_dir] = get_config("yadisk", ["yadisk_bckup_path"])

    dst_path = get_path('', backup_folder)
    dst_file = '%s.dump.gz'%(dbname)
    create_today_backup(dbname, dbuser, dst_path, dst_file)
    dir = os.listdir(dst_path)
    for a_file in dir:
        write_to_yadisk(a_file, dst_path, to_dir, '')

    stop_logging('create_backup.py')

#main
if __name__ == "__main__":
    create_backup_job()
