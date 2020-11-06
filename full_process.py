#-------------------------------------------------------------------------------
# Name:        full_process
# Purpose:
# Author:      chaus
# Created:     21.04.2019
#-------------------------------------------------------------------------------
from get_and_merge_points import get_and_merge_points_job
from send_to_dispatcher import send_to_dispatcher_job
from send_to_subscribers import send_to_subscribers_job
from week_selection import week_selection_job


def full_process():
    get_and_merge_points_job()
    send_to_dispatcher_job()
    send_to_subscribers_job()
    week_selection_job()

if __name__ == '__main__':
    full_process()
