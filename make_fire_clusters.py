#-------------------------------------------------------------------------------
# Name:        make_fire_clusters
# Purpose:
# Author:      Chaus
# Created:     18.04.2019
#-------------------------------------------------------------------------------

from falogging import log, start_logging, stop_logging
from faservice import get_config, get_cursor, close_conn

#Создаем таблицу для выгрузки дешифровщику
def make_table_for(conn,cursor,src_tab,critical,period,reg_list,point_tab,centr_tab,clust_tab,peat_tab,dist,buf):
    user = 'db_reader'
    statements = (
        """
		DROP TABLE IF EXISTS %s
		"""%(point_tab),
		"""
		CREATE TABLE %s (
                name VARCHAR(30),
                description VARCHAR(256),
                acq_date VARCHAR(10),
				acq_time VARCHAR(5),
                sat_sensor VARCHAR(5),
                region  VARCHAR(100),
				rating SMALLINT,
				critical SMALLINT,
				peat_id VARCHAR(256),
				peat_district VARCHAR(254),
				peat_class SMALLINT,
				peat_fire SMALLINT,
                geom GEOMETRY(POINT, 3857)
		)
		"""%(point_tab),
        """
        INSERT INTO %(w)s (name,acq_date,acq_time,sat_sensor,region,rating,critical,peat_id,peat_district,peat_class,peat_fire,geom)
            SELECT
                %(s)s.name,
                %(s)s.acq_date,
                %(s)s.acq_time,
                %(s)s.satellite,
                %(s)s.region,
                %(s)s.rating,
                GREATEST(%(s)s.critical,%(s)s.revision),
                %(s)s.peat_id,
                %(s)s.peat_district,
                %(s)s.peat_class,
                %(s)s.peat_fire,
                ST_Transform(%(s)s.geog::geometry,3857)::geometry
            FROM %(s)s
            WHERE date_time >= NOW() - INTERVAL '%(p)s' AND (critical >= %(c)s OR revision >= %(c)s) AND region in %(r)s
        """%{'w':point_tab,'s':src_tab,'p':period,'c':critical[clust_tab],'r':reg_list[clust_tab]},
        """
		DROP TABLE IF EXISTS %s
		"""%(clust_tab),
        """
        CREATE TABLE %(c)s
            AS SELECT
                peat_id,
                ST_Buffer(ST_ConvexHull(unnest(ST_ClusterWithin(geom, %(d)s))), %(b)s, 'quad_segs=8') AS buffer
            FROM %(s)s
            GROUP BY peat_id
        """%{'c': clust_tab, 's': point_tab, 'd': dist, 'b': buf},
        """
		DROP TABLE IF EXISTS %s
		"""%(centr_tab),
        """
        CREATE TABLE %(c)s
            AS SELECT
                peat_id,
                ST_Centroid(buffer)
            FROM %(s)s
        """%{'c': centr_tab,'s': clust_tab},
       """
		DROP TABLE IF EXISTS %s
		"""%(peat_tab),
        """
        CREATE TABLE %(p)s
            AS SELECT DISTINCT ON (peat_id) *
            FROM %(s)s
        """%{'p': peat_tab, 's': point_tab},
	"""
	GRANT SELECT ON %(t)s TO %(u)s
	"""%{'t': point_tab, 'u': user},
	"""
	GRANT SELECT ON %(t)s TO %(u)s
	"""%{'t': clust_tab, 'u': user},
	"""
	GRANT SELECT ON %(t)s TO %(u)s
	"""%{'t': centr_tab, 'u': user},
	"""
	GRANT SELECT ON %(t)s TO %(u)s
	"""%{'t': peat_tab, 'u': user}
    )
    try:
        for sql_stat in statements:
            cursor.execute(sql_stat)
            conn.commit()
        log('The table created:%s'%(clust_tab))
    except IOError as e:
        log('Error creating subscribers tables:$s'%e)
    cursor.execute("SELECT count(*) FROM %s"%(clust_tab))
    return cursor.fetchone()[0]

def make_fire_clusters_job():

    start_logging('make_fire_clusters.py')

    # extract params from config
    [year_tab] = get_config("tables", ["year_tab"])
    [clst_dist,clst_buf,clst_period] = get_config("clusters", ["cluster_dist","cluster_buf","cluster_period"])
    [point_tab, clust_tab, centr_tab, peat_tab] = get_config("tables", ["point_tab", "clust_tab", "centr_tab", "peat_tab"])
    
    critical = {'fire_clusters': 120, 'test': 120}
    #Списки регионов
    reg_list_cr = "('Ярославская область','Тверская область','Смоленская область','Рязанская область','Московская область','Москва','Калужская область','Ивановская область','Владимирская область','Брянская область')"
    reg_list_mo = "('Московская область','Москва','Смоленская область')"
    reg_list = {'fire_clusters': reg_list_cr, 'test': reg_list_mo}
    
    #connecting to database
    conn, cursor = get_cursor()

    num_points = make_table_for(conn,cursor,year_tab,critical,clst_period,reg_list,point_tab,centr_tab,clust_tab,peat_tab,clst_dist,clst_buf)
    #print('Создано %s записей'%num_points)

    close_conn(conn, cursor)
    stop_logging('make_fire_clusters.py')

#main
if __name__ == "__main__":
    make_fire_clusters_job()
