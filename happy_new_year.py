"""
Main firealert robot part.

Started via crontab: '00 04 1 1 * happy_new_year.py'

Yearly copying of point table and creating of new clear table for new year.

Created:     20.12.2019

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/

"""

import time
from falogging import log, start_logging, stop_logging
from faservice import get_config, get_cursor, close_conn


def copy_table(conn, cursor, src_tab, now_year):
    """Copy last year table to table with name '<current_table>_YYYY'."""
    dst_tab = f"{src_tab}_{str(now_year)}"
    statements = (
        f"""
        ALTER TABLE {src_tab}
        RENAME TO {dst_tab}
        """,
        f"""
        CREATE TABLE {src_tab} AS
            SELECT * FROM {dst_tab}
            WITH NO DATA
        """
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log(f"The table copied: {src_tab}")
    except IOError as err:
        log(f"Error copying year table: {err}")


def happy_new_year_job():
    """Yearly moving of data table."""
    start_logging("happy_new_year.py")
    [year_tab] = get_config("tables", ["year_tab"])
    currtime = time.localtime()
    now_year = int(time.strftime("%Y", currtime)) - 1
    conn, cursor = get_cursor()

    copy_table(conn, cursor, year_tab, now_year)

    close_conn(conn, cursor)
    stop_logging("happy_new_year.py")


if __name__ == "__main__":
    happy_new_year_job()
