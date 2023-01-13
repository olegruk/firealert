"""
Main firealert robot part.

Started via crontab: '00 9,13,17,21 * * * check_robot_health.py'

Check robot health by searching an error files in log folder.

Created:     24.01.2020

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
import time
from falogging import (
    start_logging,
    stop_logging)
from faservice import (
    get_config,
    send_to_telegram,
    send_email_with_attachment)


def is_files_exist(filelist):
    """Form a list of files with errors and its count."""
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


def rm_files(existlist):
    """Remove files specified in the list."""
    for f in existlist:
        if os.path.isfile(f):
            os.remove(f)


def check_robot_health_job():
    """Check a robot health main job."""
    start_logging("check_robot_health_job.py")

    [url, chat_id] = get_config("telegramm", ["url", "tst_chat_id"])
    [filelist, mail_addr] = get_config("health", ["filelist", "emails"])

    count, existlist = is_files_exist(filelist)
    currtime = time.localtime()

    if count == 0 and currtime.tm_hour == 9:
        msg = "В Багдаде все спокойно..."
        send_to_telegram(url, chat_id, msg)
    elif count == 1:
        msg = "Здоровье подорвано! Ошибки в 1 журнале."
        send_to_telegram(url, chat_id, msg)
    elif count > 1:
        msg = f"Держаться нету больше сил!.. Ошибки в {str(count)} журналах."
        send_to_telegram(url, chat_id, msg)

    if count > 0:
        subject = "Detected log files with errors"
        body_text = "In the attachment log files with errors.\r\n"\
                    "Check and correct current robot algorithms."
        send_email_with_attachment(mail_addr, subject, body_text, existlist)
        rm_files(existlist)

    stop_logging("check_robot_health_job.py")


if __name__ == "__main__":
    check_robot_health_job()
