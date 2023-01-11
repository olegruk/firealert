"""
Create daily backup of firealert database.

Created:     10.04.2019

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/

Restoring: pg_restore -d newdb db.dump
"""

import os
from faservice import get_config, get_path, write_to_yadisk
from falogging import start_logging, stop_logging, log


def create_today_backup(dbname, dbuser, dst_folder, dst_file):
    """Create current pgdump."""
    dst_file = os.path.join(dst_folder, dst_file)
    if os.path.isfile(dst_file):
        os.remove(dst_file)
        log(f"Owerwrite backup {dst_file}...")
    else:
        log(f"Create new backup {dst_file}...")
    command = f"pg_dump -U {dbuser} -w {dbname} > {dst_file} | \
                    split -b 250M --filter='gzip'"
    os.system(command)
    log('Done.')


def create_backup_job():
    """General function."""
    start_logging('create_backup.py')

    # extract db params from config
    [dbname, dbuser] = get_config("db", ["dbname", "dbuser"])
    [backup_folder] = get_config("path", ["backup_folder"])
    [to_dir] = get_config("yadisk", ["yadisk_bckup_path"])

    dst_path = get_path('', backup_folder)
    dst_file = f"{dbname}.dump.gz"
    create_today_backup(dbname, dbuser, dst_path, dst_file)
    dir_list = os.listdir(dst_path)
    for a_file in dir_list:
        write_to_yadisk(a_file, dst_path, to_dir, '')

    stop_logging('create_backup.py')


if __name__ == "__main__":
    create_backup_job()
