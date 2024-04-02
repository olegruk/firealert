"""
Main firealert robot part.

Started via crontab: '00 04 * * * create_backup.py'

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

Restoring: 'pg_restore -d firealert db.dump' (if one file backup)
or 'cat firealert.dump | psql -d firealert -U dbuser' (if many volumes)
"""

import os
from datetime import date, timedelta
from mylogger import init_logger
from faservice import (
    get_config,
    get_path,
    write_to_yadisk,
    remove_folder_from_yadisk)

logger = init_logger()


def create_today_backup(dbname, dbuser, dst_folder, dst_file):
    """Create current pgdump."""
    dst_file = os.path.join(dst_folder, dst_file)
    if os.path.isfile(dst_file):
        os.remove(dst_file)
        logger.info(f"Owerwrite backup {dst_file}...")
    else:
        logger.info(f"Create new backup {dst_file}...")
    command = f"pg_dump -U {dbuser} -w {dbname} | gzip > {dst_file}"
    os.system(command)
    command = f"split -d -b 99M {dst_file} {dst_file}."
    os.system(command)
    logger.info("Done.")


def create_backup_job():
    """General function."""
    logger.info("-----------------------------------")
    logger.info("Process [create_backup.py] started.")

    # extract db params from config
    [dbname, bckup_user] = get_config("db", ["dbname", "bckup_user"])
    [backup_folder] = get_config("path", ["backup_folder"])
    [to_dir] = get_config("yadisk", ["yadisk_bckup_path"])

    old_date = str(date.today() - timedelta(days=10))
    cur_date = str(date.today())

    dst_path = get_path('', backup_folder)
    dst_file = f"{dbname}.dump.gz"
    create_today_backup(dbname, bckup_user, dst_path, dst_file)
    dir_list = os.listdir(dst_path)
    dir_list.remove(dst_file)
    for a_file in dir_list:
        write_to_yadisk(a_file, dst_path, to_dir, cur_date)
        rm_file = os.path.join(dst_path, a_file)
        os.remove(rm_file)
    r_folder = to_dir + old_date
    remove_folder_from_yadisk(r_folder)
    logger.info("Process [create_backup.py] stopped.")


if __name__ == "__main__":
    create_backup_job()
