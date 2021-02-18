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

def return_json(id, request_url, token, attr_fields, n):
    params = {"token": token,
            "where": "gid>"+str(n),
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
            "returnCountOnly": "false",
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
    else:
        # Request for token must be made through HTTPS.
        if "error" in response:
            error_mess = response.get("error", {}).get("message")
            raise Exception("AGOL error: {} ".format(error_mess))

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
    dst_file = os.path.join(result_dir,'temp.json')

    #Получаем токен для доступа к сервису ArcGIS Online
    token = return_token(service_url, username, password, request_url)
    
    for id in [0]:
        log('Syncing for table №%s...'%(id))
        n = 0
        m = 0
        is_repeat = True
        while is_repeat:
            dst_file = os.path.join(result_dir,'temp-%s.json'%m)
            response = return_json(id, request_url, token, attr_fields, n)
            if id > 0:
                response['geometryType'] = 'esriGeometryMultiPolygon'
            is_repeat = 'exceededTransferLimit' in response
            with open(dst_file, 'w') as outfile:
                json.dump(response, outfile)
            n+=1000
            m+=1
            log('%s loaded...'%(n))
        for i in range(m):
            dst_file = os.path.join(result_dir,'temp-%s.json'%i)
            if i == 0:
                command = """ogr2ogr -overwrite -update -t_srs "+proj=longlat +datum=WGS84 +no_defs" -f "PostgreSQL"  PG:"host=%(h)s user=%(u)s dbname=%(b)s password=%(w)s port=%(p)s" %(s)s -nln %(d)s"""%{'s':dst_file,'d':dst_tabs[id],'h':dbserver,'u':dbuser,'b':dbname,'w':dbpass,'p':dbport}
            else:
                command = """ogr2ogr -append -update -t_srs "+proj=longlat +datum=WGS84 +no_defs" -f "PostgreSQL"  PG:"host=%(h)s user=%(u)s dbname=%(b)s password=%(w)s port=%(p)s" %(s)s -nln %(d)s"""%{'s':dst_file,'d':dst_tabs[id],'h':dbserver,'u':dbuser,'b':dbname,'w':dbpass,'p':dbport}
            os.system(command)
            log('File %s uploaded...'%(dst_file))

        log('All done in table №%(t)s. Total of %(c)s features.'%{'t': id, 'c': n})

        cursor.execute("GRANT SELECT ON %(t)s TO %(u)s"%{'t':dst_tabs[id],'u':'db_reader'})
 

    close_conn(conn, cursor)

    stop_logging('get_agol_features.py')

#main
if __name__ == "__main__":
    get_agol_features_job()