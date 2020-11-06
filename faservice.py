#-------------------------------------------------------------------------------
# Name:        fa_service
# Purpose:
# Author:      Chaus
# Created:     28.10.2020
#-------------------------------------------------------------------------------import os

from falogging import log
import os, re

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