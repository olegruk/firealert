from faservice import get_path
from faconfig import get_config
import requester, re

#[data_root,temp_folder] = get_config("path", ["data_root", "temp_folder"])
#Создаем каталог для записи временных файлов
#result_dir = get_path(data_root,temp_folder)
circle = ['55.66173821', '41.37139873', '16000']
req_params = {'lim_for':'critical', 'limit':'0', 'from_time':'NOW', 'period':'4 days'}
#dst_file = requester.request_for_circle('117419', req_params['lim_for'], req_params['limit'], req_params['from_time'], req_params['period'], circle, result_dir)
req = """f16 2020-11-07 2 years 2 month 12h 2 minutes (55.66173821, 41.37139873, 10000)"""

per = ''
yy = re.search(r'\d{1,2} year[s]{0,1}', req)
mm = re.search(r'\d{1,2} month', req)
dd = re.search(r'\d{1,2} day[s]{0,1}', req)
hh = re.search(r'\d{1,2} hour[s]{0,1}', req)
if not(hh):
    hh = re.search(r'\d{1,2}[hH]', req)
mi = re.search(r'\d{1,2} minutes', req)

for xx in [yy, mm, dd, hh, mi]:
    if xx:
        per = per + xx[0] + ' '

print(per)


y = re.search(r'\(\d{1,2}.\d{1,8},', req)
if y:
    circle[1] = y[0][1:-1]
x = re.search(r', \d{1,2}.\d{1,8},', req)
if x:
    circle[0] = x[0][2:-1]
r = re.search(r', \d{1,8}\)', req)
if r:
    circle[2] = r[0][2:-1]

print(circle)