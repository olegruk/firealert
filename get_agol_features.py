#-------------------------------------------------------------------------------
# Name:        get_agol_features
# Purpose:
# Author:      Chaus
# Created:     30.10.2020
#-------------------------------------------------------------------------------

import os, json, contextlib, requests
from urllib.request import urlopen
from urllib.parse import urlencode
from falogging import log, start_logging, stop_logging
from faservice import get_path, get_db_config, get_config, get_cursor, close_conn

#dst_tabs = ['oopt_reg_points', 'oopt_reg_polygons', 'oopt_reg_clusters', 'oopt_reg_zones']
#attr_fields = ["gid,id,status,category,adm_rf,name,actuality,cluster,location,designatio,year,source,scale",
#        "gid,id,status,category,region,name,actuality,area_ha,date,year,source,scale",
#        "gid,id,status,category,region,name,actuality,id_cluster,descriptio,name_clust,area_ha,date",
#        "gid,id,status,category,region,name,actuality,id_cluster,descriptio,name_clust,area_ha,date"]

def submit_request(request):
    """ Returns the response from an HTTP request in json format."""
    with contextlib.closing(urlopen(request)) as response:
        result = json.load(response)
        return result

def submit_request1(url, params):
    response = requests.post(url + 'sendMessage', data=params)
    if response.status_code != 200:
        raise Exception("post_text error: %s" %response.status_code)
    resp_text = json.loads (response.text)
    return resp_text

def return_token(service_url, username, password, request_url):
    """ Returns an authentication token for use in ArcGIS Online."""
    log('Requesting token...')
    params = {"username": username,
            "password": password,
            "referer": request_url,
            "f": "json"}
    response = requests.post(service_url + 'sendMessage', data=params)
    if response.status_code != 200:
        raise Exception("post_text error: %s" %response.status_code)
    token_response = json.loads (response.text)
    if "token" in token_response:
        token = token_response.get("token")
        log('Token requested.')
        return token
    else:
        # Request for token must be made through HTTPS.
        if "error" in token_response:
            error_mess = token_response.get("error", {}).get("message")
            if "This request needs to be made over https." in error_mess:
                token_url = token_url.replace("http://", "https://")
                token = return_token(service_url, username, password, request_url)
                return token
            else:
                raise Exception("AGOL error: {} ".format(error_mess))

def return_json(id, request_url, token, attr_fields, count, n1, n2):
    params = {"token": token,
            "where": "id>"+str(n1)+" and id<="+str(n2),
            "objectIds": "",
            "time": "",
            "geometry": "",
            "geometryType": "esriGeometryEnvelope",
            "inSR": "",
            "spatialRel": "esriSpatialRelIntersects",
            "distance": "",
            "units": "esriSRUnit_Meter",
            "relationParam": "",
            "outFields": attr_fields[id],
            "returnGeometry": "true",
            "maxAllowableOffset": "",
            "geometryPrecision": "",
            "outSR": "",
            "gdbVersion": "",
            "returnDistinctValues": "false",
            "returnIdsOnly": "false",
            "returnCountOnly": count,
            "returnExtentOnly": "false",
            "orderByFields": "",
            "groupByFieldsForStatistics": "",
            "outStatistics": "",
            "returnZ": "false",
            "returnM": "false",
            "multipatchOption": "",
            "resultOffset": "",
            "resultRecordCount": "",
            "f": "pjson"}

    request = "{}/{}/query?{}".format(request_url, str(id), urlencode(params))
    response = submit_request(request)
    if "features" in response:
        return response
    elif "count" in response:
        return response.get('count')
    else:
        # Request for token must be made through HTTPS.
        if "error" in response:
            error_mess = response.get("error", {}).get("message")
            raise Exception("AGOL error: {} ".format(error_mess))

#Удаляем таблицы
def drop_tables(conn,cursor, tab):
    log("Dropping table %s..." %tab)
    sql_stat = "DROP TABLE IF EXISTS %s"%(tab)
    try:
        cursor.execute(sql_stat)
        conn.commit()
        log("Tables dropped.")
    except IOError as e:
        log('Error dropping table:$s'%e)

def drop_temp_files(result_dir):
    for the_file in os.listdir(result_dir):
        file_path = os.path.join(result_dir, the_file)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            log('Cannot remove files:$s' %e)

def get_agol_features_job():
 
    start_logging('get_agol_features.py')

    # extract params from config
    [dbserver,dbport,dbname,dbuser,dbpass] = get_db_config()
    [data_root,temp_folder] = get_config("path", ["data_root", "temp_folder"])
    [username, password, service_url, request_url] = get_config("aari", ["username", "password", "service_url", "request_url"])
    [dst_tabs, attr_fields] = get_config("aari", ["dst_tabs", "attr_fields"])

    #connecting to database
    conn, cursor = get_cursor()
    
    #Создаем каталог для записи временных файлов
    result_dir = get_path(data_root,temp_folder)
    
    for id in [2, 3, 0, 1]:
#    for id in [1]:
        log('Syncing for table №%s...'%(id))
        #Получаем токен для доступа к сервису ArcGIS Online
        token = return_token(service_url, username, password, request_url)
        n = 0
        m = 1
        loaded = 0
        full_count = return_json(id, request_url, token, attr_fields, "true", 0, 100000000)
#        print(full_count)
        while loaded < full_count:
            curr_count = return_json(id, request_url, token, attr_fields, "true", n, n+1000)
            if curr_count > 0:
                response = return_json(id, request_url, token, attr_fields, "false", n, n+1000)
                dst_file = os.path.join(result_dir,'temp-%s.json'%m)
                if id > 0:
                    response['geometryType'] = 'esriGeometryMultiPolygon'
                with open(dst_file, 'w') as outfile:
                    json.dump(response, outfile)
                m+=1
                loaded+=curr_count
                log('Loaded: %(l)s, summary: %(s)s'%{'l': curr_count, 's': loaded})
#                print('Loaded: %(l)s, summary: %(s)s'%{'l': curr_count, 's': loaded})
            n+=1000
        drop_tables(conn, cursor, dst_tabs[id])
        for i in range(1, m):
            dst_file = os.path.join(result_dir,'temp-%s.json'%i)
            if i == 0:
                command = """ogr2ogr -overwrite -update -t_srs "+proj=longlat +datum=WGS84 +no_defs" -f "PostgreSQL"  PG:"host=%(h)s user=%(u)s dbname=%(b)s password=%(w)s port=%(p)s" %(s)s -nln %(d)s"""%{'s':dst_file,'d':dst_tabs[id],'h':dbserver,'u':dbuser,'b':dbname,'w':dbpass,'p':dbport}
            else:
                command = """ogr2ogr -append -update -t_srs "+proj=longlat +datum=WGS84 +no_defs" -f "PostgreSQL"  PG:"host=%(h)s user=%(u)s dbname=%(b)s password=%(w)s port=%(p)s" %(s)s -nln %(d)s"""%{'s':dst_file,'d':dst_tabs[id],'h':dbserver,'u':dbuser,'b':dbname,'w':dbpass,'p':dbport}
            os.system(command)
            log('File %s uploaded...'%(dst_file))
#            print('File %s uploaded...'%(dst_file))

        log('All done in table №%(t)s. Total of %(c)s features.'%{'t': id, 'c':loaded})
#        print('All done in table №%(t)s. Total of %(c)s features.'%{'t': id, 'c':loaded})

        cursor.execute("GRANT SELECT ON %(t)s TO %(u)s"%{'t':dst_tabs[id],'u':'db_reader'})
        drop_temp_files(result_dir)

    close_conn(conn, cursor)

    stop_logging('get_agol_features.py')

#main
if __name__ == "__main__":
    get_agol_features_job()