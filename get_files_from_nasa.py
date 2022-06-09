import time
from falogging import start_logging, stop_logging
from faservice import get_config, get_path
from get_and_merge_points import GetPoints_with_retries


def get_points_job():
    currtime = time.localtime()
    date=time.strftime('%Y-%m-%d_%H-%M',currtime)

    start_logging('get_files_from_nasa.py')

    [data_root,firms_folder] = get_config("path", ["data_root", "firms_folder"])
    [pointsets] = get_config("sources", ["src"])
    firms_path = get_path(data_root,firms_folder)


    for pointset in pointsets:
        errcode = GetPoints_with_retries(pointset, firms_path, date)
    
    stop_logging('get_files_from_nasa.py')

#main
if __name__ == "__main__":
    get_points_job()