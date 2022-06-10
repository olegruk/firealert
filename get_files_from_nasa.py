import os, time
import requests, shutil
from requests.exceptions import HTTPError
from falogging import log, start_logging, stop_logging

def read_csv_from_site(url,sourcepath):
    try:
        filereq = requests.get(url,stream = True)
        filereq.raise_for_status()
    except HTTPError as http_err:
        log(f'HTTP error occurred: {http_err}')  # Python 3.6
        errcode = 2
    except Exception as err:
        log(f'Other error occurred: {err}')  # Python 3.6
        errcode = 3
    else:
        log(f'Get receive status code {filereq.status_code}')
        #filereq = requests.get(url,stream = True,verify=False)
        with open(sourcepath,"wb") as receive:
            shutil.copyfileobj(filereq.raw,receive)
            del filereq
        errcode = 0
    return errcode

#Процедура получения csv-файла точек
def GetPoints(pointset, dst_folder, aDate):
    log("Getting points for %s..." %pointset)
    src_urls = {
        'as_modis': 'https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_Russia_Asia_24h.csv',
        'eu_modis': 'https://firms.modaps.eosdis.nasa.gov/data/active_fire/modis-c6.1/csv/MODIS_C6_1_Europe_24h.csv',
        'as_viirs': 'https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Russia_Asia_24h.csv',
        'eu_viirs': 'https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Europe_24h.csv',
        'as_vnoaa': 'https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_Russia_Asia_24h.csv',
        'eu_vnoaa': 'https://firms.modaps.eosdis.nasa.gov/data/active_fire/noaa-20-viirs-c2/csv/J1_VIIRS_C2_Europe_24h.csv'
    }
    dst_file = "%s_%s.csv"%(pointset,aDate)
    dst_file = os.path.join(dst_folder, dst_file)
    errcode = read_csv_from_site(src_urls[pointset],dst_file)
    if errcode == 0:
        log("Download complete: %s" %dst_file)
    else:
        if os.path.exists(dst_file):
            try:
                os.remove(dst_file)
            except OSError as error:
                log("Unable to remove file: %s" %dst_file)
        log("Not downloaded: %s" %dst_file)
    return errcode

def get_points_job():
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d_%H-%M',currtime)

    start_logging('get_files_from_nasa.py')

    data_root = 'data'
    firms_folder = 'firms'
    pointsets = ['as_modis', 'eu_modis', 'as_viirs', 'eu_viirs', 'as_vnoaa', 'eu_vnoaa']
    base_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(base_path, data_root)
    if not os.path.exists(result_path):
        try:
            os.mkdir(result_path)
        except OSError:
            log("Unable to create %s" %result_path)
    firms_path = os.path.join(result_path, firms_folder)
    if not os.path.exists(firms_path):
        try:
            os.mkdir(firms_path)
        except OSError:
            log("Unable to create %s" %firms_path)
 
    for pointset in pointsets:
        GetPoints(pointset, firms_path, date)
    
    stop_logging('get_files_from_nasa.py')

#main
if __name__ == "__main__":
    get_points_job()