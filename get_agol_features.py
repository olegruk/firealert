"""
Additional firealert robot part.

Started manual.
Update OOPT zones from aari, using ArcGIS online services.

Created:     30.10.2020

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
import json
import contextlib
import requests
from urllib.request import urlopen
from urllib.parse import urlencode
from faservice import (
    get_path,
    get_db_config,
    get_config,
    get_cursor,
    close_conn)
from mylogger import init_logger

# dst_tabs = ['oopt_reg_points',
#             'oopt_reg_polygons',
#             'oopt_reg_clusters',
#             'oopt_reg_zones']
# attr_fields = ["gid, id, status, category, adm_rf, name, actuality,
#                 cluster,location, designatio, year, source, scale",
#                "gid, id, status, category, region, name, actuality,
#                 area_ha, date, year, source, scale",
#                "gid, id, status, category, region, name, actuality,
#                 id_cluster, descriptio, name_clust, area_ha, date",
#                "gid, id, status, category, region, name, actuality,
#                 id_cluster, descriptio, name_clust, area_ha, date"]

logger = init_logger()


def submit_request(request):
    """Return the response from an HTTP request in json format."""
    with contextlib.closing(urlopen(request)) as response:
        result = json.load(response)
        return result


def submit_request1(url, params):
    """Return the response from an HTTP request in json format.

    Deprecated.
    """
    response = requests.post(f"{url}sendMessage", data=params)
    if response.status_code != 200:
        raise Exception(f"post_text error: {response.status_code}")
    resp_text = json.loads(response.text)
    return resp_text


def return_token(service_url, username, password, request_url):
    """Return an authentication token for use in ArcGIS Online."""
    logger.info('Requesting token...')
    params = {"username": username,
              "password": password,
              "referer": request_url,
              "f": "json"}
    response = requests.post(f"{service_url}sendMessage", data=params)
    if response.status_code != 200:
        raise Exception(f"post_text error: {response.status_code}")
    token_response = json.loads(response.text)
    if "token" in token_response:
        token = token_response.get("token")
        logger.info("Token requested.")
        return token
    else:
        # Request for token must be made through HTTPS.
        if "error" in token_response:
            error_mess = token_response.get("error", {}).get("message")
            if "This request needs to be made over https." in error_mess:
                service_url = service_url.replace("http://", "https://")
                token = return_token(service_url,
                                     username,
                                     password,
                                     request_url)
                return token
            else:
                raise Exception("AGOL error: {} ".format(error_mess))


def return_json(id, request_url, token, attr_fields, count, n1, n2):
    """Request data from ArcGIS online. Return data in json format."""
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

    request = f"{request_url}/{str(id)}/query?{urlencode(params)}"
    response = submit_request(request)
    if "features" in response:
        return response
    elif "count" in response:
        return response.get("count")
    else:
        # Request for token must be made through HTTPS.
        if "error" in response:
            error_mess = response.get("error", {}).get("message")
            raise Exception(f"AGOL error: {error_mess} ")


def drop_tables(conn, cursor, tab):
    """Drop temporary tables."""
    logger.info(f"Dropping table {tab}...")
    sql_stat = f"DROP TABLE IF EXISTS {tab}"
    try:
        cursor.execute(sql_stat)
        conn.commit()
        logger.info("Tables dropped.")
    except IOError as err:
        logger.error(f"Error dropping table: {err}")


def drop_temp_files(result_dir):
    """Drop temporary files."""
    for the_file in os.listdir(result_dir):
        file_path = os.path.join(result_dir, the_file)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
            # elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as err:
            logger.error(f"Cannot remove files: {err}")


def get_agol_features_job():
    """Get features main job."""
    logger.info("---------------------------------------")
    logger.info("Process [get_agol_features.py] started.")

    [dbserver, dbport, dbname, dbuser, dbpass] = get_db_config()
    [data_root, temp_folder] = get_config(
        "path", ["data_root", "temp_folder"])
    [username, password, service_url, request_url] = get_config(
        "aari", ["username", "password", "service_url", "request_url"])
    [dst_tabs, attr_fields] = get_config(
        "aari", ["dst_tabs", "attr_fields"])
    conn, cursor = get_cursor()
    result_dir = get_path(data_root, temp_folder)

    for id in [2, 3, 0, 1]:
        logger.info(f"Syncing for table №{id}...")
        # Get an access token for ArcGIS Online
        token = return_token(service_url, username, password, request_url)
        n = 0
        m = 1
        loaded = 0
        full_count = return_json(id, request_url, token,
                                 attr_fields, "true", 0, 100000000)
        while loaded < full_count:
            curr_count = return_json(id, request_url, token,
                                     attr_fields, "true", n, n+1000)
            if curr_count > 0:
                response = return_json(id, request_url, token,
                                       attr_fields, "false", n, n+1000)
                dst_file = os.path.join(result_dir, f"temp-{m}.json")
                if id > 0:
                    response["geometryType"] = "esriGeometryMultiPolygon"
                with open(dst_file, "w") as outfile:
                    json.dump(response, outfile)
                m += 1
                loaded += curr_count
                logger.info(f"Loaded: {curr_count}, summary: {loaded}")
            n += 1000
        drop_tables(conn, cursor, dst_tabs[id])
        for i in range(1, m):
            dst_file = os.path.join(result_dir, f"temp-{i}.json")
            if i == 0:
                command = f"""ogr2ogr \
                                -overwrite \
                                -update \
                                -t_srs "+proj=longlat +datum=WGS84 +no_defs" \
                                -f "PostgreSQL" \
                                    PG:"host={dbserver} \
                                        user={dbuser} \
                                        dbname={dbname} \
                                        password={dbpass} \
                                        port={dbport}" \
                                {dst_file} \
                                -nln \
                                {dst_tabs[id]}"""
            else:
                command = f"""ogr2ogr \
                                -append \
                                -update \
                                -t_srs "+proj=longlat +datum=WGS84 +no_defs" \
                                -f "PostgreSQL" \
                                    PG:"host={dbserver} \
                                    user={dbuser} \
                                    dbname={dbname} \
                                    password={dbpass} \
                                    port={dbport}" \
                                {dst_file} \
                                -nln \
                                {dst_tabs[id]}"""
            os.system(command)
            logger.info(f"File {dst_file} uploaded...")

        logger.info(f"All done in table №{id}. Total of {loaded} features.")

        cursor.execute(f"GRANT SELECT ON {dst_tabs[id]} TO {'db_reader'}")
        drop_temp_files(result_dir)

    close_conn(conn, cursor)
    logger.info("Process [get_agol_features.py] stopped.")


if __name__ == "__main__":
    get_agol_features_job()
