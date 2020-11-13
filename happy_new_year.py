#-------------------------------------------------------------------------------
# Name:        happy_new_year
# Purpose:
# Author:      Chaus
# Created:     20.12.2019
#-------------------------------------------------------------------------------

import time
from falogging import log, start_logging, stop_logging
from faservice import get_config, get_cursor, close_conn

def copy_table(conn,cursor,src_tab,now_year):
    dst_tab = src_tab + '_' + str(now_year)
    statements = (
    """
    ALTER TABLE %(s)s RENAME TO %(d)s
    """%{'s': src_tab, 'd': dst_tab},
#   """
#	DROP TABLE IF EXISTS %s
#	"""%(src_tab),
    """
    CREATE TABLE %(s)s AS
        SELECT * FROM %(d)s
        WITH NO DATA
    """%{'s': src_tab, 'd': dst_tab}
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log('The table copied:%s'%(src_tab))
    except IOError as e:
        log('Error copying year table:$s'%e)

def happy_new_year_job():

    start_logging('happy_new_year.py')
    
    # extract db params from config
    [year_tab] = get_config("tables", ["year_tab"])
    currtime = time.localtime()
    now_year = int(time.strftime('%Y',currtime))-1

    #connecting to database
    conn, cursor = get_cursor()

    copy_table(conn,cursor,year_tab,now_year)

    close_conn(conn, cursor)
    stop_logging('happy_new_year.py')

#main
if __name__ == '__main__':
    happy_new_year_job()
