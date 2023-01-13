"""
Init my customized logger based on logging unit.

Created:     11.01.2023

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
import re
import logging
import traceback
from logging import StreamHandler
import requests

url = "https://api.telegram.org/bot"\
      "990586097:AAHQ8uKZ2q_usZLMDPkbUfFfBJ6-8GLvvlk/"
user_id = "-1001416479771"


class TgLoggerHandler(StreamHandler):
    """Logger handler for tlg_logger."""

    def __init__(self, bot_url: str, chat_id: str, timeout: int = 10):
        """Initialise TgLoggerHandler class."""
        super().__init__()
        self.url = bot_url
        self.chat_id = chat_id
        self.timeout = timeout

    def emit(self, record):
        """Send msg to telegram."""
        msg = self.format(record)
        params = {'chat_id': self.chat_id, 'text': msg}
        response = requests.post(self.url + '/sendMessage',
                                 data=params,
                                 timeout=self.timeout)
        if response.status_code != 200:
            raise Exception(f"post_text error: {response.status_code}")


def init_logger():
    """Actions to prepare for logging."""
    base_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(base_path, 'log')
    if not os.path.exists(result_path):
        try:
            os.mkdir(result_path)
            # log("Created %s" % result_path)
        except OSError:
            pass
            # log("Unable to create %s" % result_path)
    currtime = time.localtime()
    date = time.strftime('%Y-%m-%d', currtime)
    root_path = traceback.StackSummary.extract(
        traceback.walk_stack(None))[-1][0]
    uname = re.search(r'\w+\.py', root_path)[0][0:-3]
    logfile = f"{date}_{uname}.log"
    fulllog = os.path.join(result_path, logfile)
    a_logger = logging.getLogger(uname)
    a_logger.setLevel(logging.INFO)
    log_handler = logging.FileHandler(fulllog, mode='a')
    # log_formatter = logging.Formatter(
    #     "%(name)s %(asctime)s %(levelname)s %(message)s")
    log_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - "
        "%(filename)s - %(funcName)s(%(lineno)d) - %(message)s")
    log_handler.setFormatter(log_formatter)
    a_logger.addHandler(log_handler)
    tlg_formatter = logging.Formatter(
        "Error in module <%(name)s>. Level: %(levelname)s.\n%(message)s")
    tlg_handler = TgLoggerHandler(url, user_id)
    tlg_handler.setFormatter(tlg_formatter)
    tlg_handler.setLevel(logging.ERROR)
    a_logger.addHandler(tlg_handler)
    return a_logger


def get_logger():
    """Get existing logger."""
    root_path = traceback.StackSummary.extract(
        traceback.walk_stack(None))[-1][0]
    uname = re.search(r'\w+\.py', root_path)[0][0:-3]
    a_logger = logging.getLogger(uname)
    return a_logger
