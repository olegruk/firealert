#-------------------------------------------------------------------------------
# Name:        check_zones
# Purpose:
# Author:      Chaus
# Created:     06.02.2020
#-------------------------------------------------------------------------------

from faservice import get_config, get_cursor, close_conn, send_to_telegram
from falogging import start_logging, stop_logging, log

def check_vip_zones(conn, cursor, year_tab, dst_tab, outline, period):
    log("Checking VIP-zones...")
    statements = (
		"""
		DROP TABLE IF EXISTS %s
		"""%(dst_tab),
		"""
		CREATE TABLE %(d)s
			AS SELECT %(s)s.ident, %(o)s.name AS zone_name, %(s)s.geog
				FROM %(s)s, %(o)s
				WHERE (%(s)s.date_time > TIMESTAMP 'today' - INTERVAL '%(p)s') AND (%(s)s.vip IS NULL) AND (ST_Intersects(%(o)s.geog, %(s)s.geog))
		"""%{'d':dst_tab, 's':year_tab, 'o':outline, 'p':period},
        """
    	UPDATE %(y)s
		SET vip = 1
        FROM %(d)s
        WHERE %(d)s.ident = %(y)s.ident
		"""%{'d':dst_tab, 'y':year_tab}
        )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log('The table created:%s'%(dst_tab))
    except IOError as e:
        log('Error intersecting points with region:$s'%e)
    cursor.execute("SELECT count(*) FROM %s"%(dst_tab))
    points_count = cursor.fetchone()[0]
    #cursor.execute("SELECT DISTINCT zone_name FROM %s"%(dst_tab))
    cursor.execute("SELECT zone_name, COUNT(*) FROM %s GROUP BY zone_name"%(dst_tab))
    zones = cursor.fetchall()
    return points_count, zones

def check_zones_job():

    start_logging('check_zones.py')
    
    # extract params from config
    [year_tab] = get_config("tables", ["year_tab"])
    [url, chat_id] = get_config("telegramm", ["url", "wrk_chat_id"])
    [period] = get_config('statistic', ['period'])
    [outline] = get_config('tables', ['vip_zones'])
    dst_tab = year_tab + '_vip'

    conn, cursor = get_cursor()

    points_count, zones = check_vip_zones(conn, cursor, year_tab, dst_tab, outline, period)

    if points_count > 0:
        msg = 'Новых точек\r\nв зонах особого внимания: %s\r\n\r\n' %points_count
        for (zone, num_points) in zones:
            msg = msg + '%s - %s\r\n' %(zone, num_points)

        send_to_telegram(url, chat_id, msg)

    close_conn(conn, cursor)
    stop_logging('check_zones.py')
    
#main
if __name__ == "__main__":
    check_zones_job()
