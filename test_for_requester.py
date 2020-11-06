import requester
import falogging, faconfig
import os, time, sys, re

def get_path(root_path,folder):
    falogging.log("Creating folder %s..." %folder)
    base_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(base_path, root_path)
    result_path = os.path.join(result_path, folder)
    if not os.path.exists(result_path):
        try:
            os.mkdir(result_path)
            falogging.log("Created %s" %result_path)
        except OSError:
            falogging.log("Unable to create %s" %result_path)
    return result_path

def drop_temp_files(result_dir):
    for the_file in os.listdir(result_dir):
        file_path = os.path.join(result_dir, the_file)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            falogging.log('Cannot remove files:$s' %e)

def parse_data_req(req):
    central_region = "('Москва','Московская область','Тверская область','Ярославская область','Ивановская область','Владимирская область','Рязанская область','Тульская область','Калужская область','Брянская область','Смоленская область')"
    req_params = {'lim_for':'critical', 'limit':'0', 'from_time':'NOW', 'period':'24', 'regions':central_region}
    c_lim = re.search(r'[c,C]\d{1,3}', req)
    if c_lim:
        req_params['lim_for'] = 'critical'
        req_params['limit'] = c_lim[0][1:]
    f_lim = re.search(r'[f,F]\d{1,3}', req)
    if f_lim:
        req_params['lim_for'] = 'peat_fire'
        req_params['limit'] = f_lim[0][1:]
    dat = re.search(r'\d\d-\d\d', req)
    tme = re.search(r'\d\d:\d\d', req)
    if dat and tme:
        currtime = time.localtime()
        curryear = time.strftime('%Y',currtime)
        req_params['from_time'] = curryear + '-' + dat[0] + ' ' + tme[0]
    if dat and not(tme):
        currtime = time.localtime()
        curryear = time.strftime('%Y',currtime)
        req_params['from_time'] = curryear + '-' + dat[0] + ' ' + '23:59'
    if not(dat) and tme:
        currtime = time.localtime()
        currdate = time.strftime('%Y-%m-%d',currtime)
        req_params['from_time'] = currdate + ' ' + tme[0]
    per = re.search(r'\d{1,3}[h,H]', req)
    if per:
        req_params['period'] = per[0][0:-1]
    reg = re.search(r'\((\'\w+ ?\w* ?\w*\'\,? ?)+\)', req)
    if reg:
        req_params['regions'] = reg[0]
    return req_params

def main():
    [data_root,temp_folder] = faconfig.get_config("path", ["data_root", "temp_folder"])
    #Создаем каталог для записи временных файлов
    result_dir = get_path(data_root,temp_folder)
    whom = '580325825'
    #req = "/get_data f32 12-02 22:00 24h ('Московская область', 'Тульская область', 'Якутия', 'Республика Марий Эл')"
    req = "/get_data 23h c8 ('Московская область', 'Тульская область', 'Якутия', 'Республика Марий Эл')"
    #req = "/get_data"
    req_params = parse_data_req(req)
    print(req_params)
    #requester.request_data(whom, req_params['lim_for'], req_params['limit'], req_params['from_time'], req_params['period'], req_params['regions'], result_dir)

    #drop_temp_files(result_dir)
if __name__ == '__main__':
    main()
