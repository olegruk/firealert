from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
import subsconf, requester
import os, time, re
#import logging

from faservice import get_config, get_path, send_doc_to_telegram
from falogging import log

[url, TOKEN] = get_config('telegramm', ['url', 'token'])
#user_id = "580325825" #"Это я

# Enable logging
#logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#                    level=logging.INFO)
#
#logger = logging.getLogger(__name__)

command_list = ['',
                '',
                '/activate, /deactivate \n',
                '/add_region, /del_region, /show_regions \n',
                '/add_email, /del_email, /show_emails \n',
                '',
                '/m_subs_stat, /m_unsubs_stat \n',
                '/t_subs_stat, /t_unsubs_stat \n',
                '/m_subs_point, /m_unsubs_point \n',
                '/t_subs_point, /t_unsubs_point \n',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '']

def parse_data_req(req):
    central_region = "('Москва','Московская область','Тверская область','Ярославская область','Ивановская область','Владимирская область','Рязанская область','Тульская область','Калужская область','Брянская область','Смоленская область')"
    circle = ['', '', ''] 
    req_params = {'lim_for':'critical', 'limit':'0', 'from_time':'NOW', 'period':'24 hours', 'regions':central_region, 'circle':circle}

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
    if per != '':
        req_params['period'] = per
    
    reg = re.search(r'\((\'\w+ ?\w* ?\w*\'\,? ?)+\)', req)
    if reg:
        req_params['regions'] = reg[0]

    y = re.search(r'\(\d{1,2}.\d{1,8},', req)
    if y:
        circle[1] = y[0][1:-1]
    x = re.search(r', \d{1,2}.\d{1,8},', req)
    if x:
        circle[0] = x[0][2:-1]
    r = re.search(r', \d{1,8}\)', req)
    if r:
        circle[2] = r[0][2:-1]
    req_params['circle'] = circle

    return req_params

def drop_temp_files(result_dir):
    for the_file in os.listdir(result_dir):
        file_path = os.path.join(result_dir, the_file)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            log('Cannot remove files:$s' %e)

def start(bot, update):
    """Send a message when the command /start is issued."""
    #user = update.message.from_user
    #send = f"User {user} started your bot. \n User ID:{user.id}"
    #bot.send_message(chat_id=user_id, text=send)
    telegram_id = update.message.from_user.id
    res = subsconf.add_tlg_user(telegram_id)
    if not(res):
        update.message.reply_text('Мы с вами уже знакомы.\nПовторно давать команду /start нет необходимости.')
    else:
        update.message.reply_text('Вы включены в список подписчиков.')

def test(bot, update):
    """Send a test reply message when the command /test is issued."""
    #user = update.message.from_user
    message = update.message
    #send = f"User {user} started your bot. \n User ID:{user.id}"
    #bot.send_message(chat_id=user_id, text=send)
    #telegram_id = update.message.from_user.id
    #update.message.reply_text(message)
    print(message)

def t_subs_stat(bot, update):
    """Subscribe for statistic on telegram when the command /t_subs_stat is issued."""
    telegram_id = update.message.from_user.id
    subsconf.set_teleg_stat(telegram_id)
    update.message.reply_text('Вы подписаны на рассылку статистики в телеграм.')

def t_unsubs_stat(bot, update):
    """Unsubscribe statistic on telegram when the command /t_unsubs_stat is issued."""
    telegram_id = update.message.from_user.id
    subsconf.unset_teleg_stat(telegram_id)
    update.message.reply_text('Вы отписаны от рассылки статистики в телеграм.')

def t_subs_point(bot, update):
    """Subscribe for firepoints on telegram when the command /t_subs_point is issued."""
    telegram_id = update.message.from_user.id
    subsconf.set_teleg_point(telegram_id)
    update.message.reply_text('Вы подписаны на рассылку термоточек в телеграм.')

def t_unsubs_point(bot, update):
    """Unsubscribe firepoints on telegram when the command /t_unsubs_point is issued."""
    telegram_id = update.message.from_user.id
    subsconf.unset_teleg_point(telegram_id)
    update.message.reply_text('Вы отписаны от рассылки термоточек в телеграм.')

def activate(bot, update):
    """Activate subscribtion when the command /activate is issued."""
    telegram_id = update.message.from_user.id
    subsconf.set_active(telegram_id)
    update.message.reply_text('Подписка активирована.')

def deactivate(bot, update):
    """Deactivate subscribtion when the command /deactivate is issued."""
    telegram_id = update.message.from_user.id
    subsconf.unset_active(telegram_id)
    update.message.reply_text('Подписка отключена.')

def add_email(bot, update):
    telegram_id = update.message.from_user.id
    message = update.message.text
    email = message.split(' ', 1)[1]
    maillist = subsconf.add_new_email(telegram_id, email)
    update.message.reply_text('Список адресов обновлен:\n%s' %maillist)

def del_email(bot, update):
    telegram_id = update.message.from_user.id
    message = update.message.text
    email = message.split(' ', 1)[1]
    maillist = subsconf.remove_email(telegram_id, email)
    update.message.reply_text('Список адресов обновлен:\n%s' %maillist)

def show_emails(bot, update):
    telegram_id = update.message.from_user.id
    maillist = subsconf.show_maillist(telegram_id)
    update.message.reply_text('Список рассылки:\n%s' %maillist)

def add_region(bot, update):
    telegram_id = update.message.from_user.id
    message = update.message.text
    region = message.split(' ', 1)[1]
    reglist = subsconf.add_new_region(telegram_id, region)
    update.message.reply_text('Список регионов обновлен:\n%s' %reglist)

def del_region(bot, update):
    telegram_id = update.message.from_user.id
    message = update.message.text
    region = message.split(' ', 1)[1]
    reglist = subsconf.remove_region(telegram_id, region)
    update.message.reply_text('Список регионов обновлен:\n%s' %reglist)

def show_regions(bot, update):
    telegram_id = update.message.from_user.id
    reglist = subsconf.show_reglist(telegram_id)
    update.message.reply_text('Список регионов:\n%s' %reglist)

def list_regions(bot, update):
    reglist = subsconf.list_reglist()
    update.message.reply_text('Список возможных регионов:\n%s' %reglist)

def show_conf(bot, update):
    telegram_id = update.message.from_user.id
    conf = subsconf.show_conf(telegram_id)
    for i in range(0,21):
        paramstr = '%(s)s\n%(c)s%(p)s' %{'s':subsconf.conf_desc[i],'c':command_list[i],'p':conf[i]}
        update.message.reply_text(paramstr)

def get_data(bot, update):
    [data_root,temp_folder] = get_config("path", ["data_root", "temp_folder"])
    #Создаем каталог для записи временных файлов
    result_dir = get_path(data_root,temp_folder)

    telegram_id = update.message.from_user.id
    message = update.message.text

    req_params = parse_data_req(message)
    dst_file, nump = requester.request_data(telegram_id, req_params['lim_for'], req_params['limit'], req_params['from_time'], req_params['period'], req_params['regions'], result_dir)

    #drop_temp_files(result_dir)
    doc = open(dst_file, 'rb')
    send_doc_to_telegram(url, telegram_id, doc)
    update.message.reply_text('В файле %s точек.' %nump)

def get_around(bot, update):
    [data_root,temp_folder] = get_config("path", ["data_root", "temp_folder"])
    #Создаем каталог для записи временных файлов
    result_dir = get_path(data_root,temp_folder)

    telegram_id = update.message.from_user.id
    message = update.message.text

    req_params = parse_data_req(message)
    dst_file, nump = requester.request_for_circle(telegram_id, req_params['lim_for'], req_params['limit'], req_params['from_time'], req_params['period'], req_params['circle'], result_dir)

    #drop_temp_files(result_dir)
    doc = open(dst_file, 'rb')
    send_doc_to_telegram(url, telegram_id, doc)
    str_nump = str(nump)
    if str_nump[-1] == '1':
        tail = 'точка'
    elif str_nump[-1] in ['2','3','4']:
        tail = 'точки'
    else:
        tail = 'точек'
    update.message.reply_text('В файле %(p)s %(t)s.' %{'p':str_nump, 't':tail})

def help(bot, update):
    """Send a message when the command /help is issued."""
    update.message.reply_text('Что тут можно делать: \n'
                              '/start - \n'
                              'добавить себя в список подписчиков; \n'
                              '/t_subs_stat - \n'
                              'подписаться на получение сводной статистики в телеграм; \n'
                              '/t_unsubs_stat - \n'
                              'отказаться от подписки на получение сводки в телеграм; \n'
                              '/t_subs_point - \n'
                              'подписаться на получение термоточек в телеграм; \n'
                              '/t_unsubs_point - \n'
                              'отказаться от подписки на получение термоточек в телеграм; \n'
                              '/add_email <email> - \n'
                              'добавить адрес для почтовой рассылки; \n'
                              '/del_email - <email> \n'
                              'убрать адрес из списка адресатов; \n'
                              '/show_emails - \n'
                              'показать список адресатов рассылки; \n'
                              '/add_region - <region> \n'
                              'добавить новый регион для мониторинга термоточек; \n'
                              '/del_region - <region> \n'
                              'убрать регион из списка регионов; \n'
                              '/show_regions - \n'
                              'показать список регионов; \n'
                              '/list_regions - \n'
                              'посмотреть, какие регионы бывают;'
                              '/show_conf - \n'
                              'показать параметры моей рассылки; \n'
                              '/get_data - \n'
                              'запросить точки (подробнее - /help_get_data); \n'
                              '/get_arround - \n'
                              'запросить точки в радиусе (подробнее - /help_get_around).')

def help_get_data(bot, update):
    """Send a message when the command /help_get_data is issued."""
    update.message.reply_text('Строка запроса термоточек выглядит так: \n'
                              '/get_data c120 10-19 18:00 36h (\'Московская область\', \'Иркутская область\', \'Красноярский край\') \n'
                              'Такой запрос вернет точки с критичностью не ниже 120, \n'
                              'начиная с 18:00 19 октября, за 36 часов ("в прошлое"), \n'
                              'по Московской и Иркутской областям и Краснодарскому краю. \n'
                              'Можно заменить букву "с" буквой "f", например - "f32," \n'
                              'это будет означать точки, попавшие в области торфяников с горимостью не ниже 32. \n'
                              'Если не указаны ни "c", ни "f", то предполагается значение "c0", \n'
                              'если не указано время, то оно будет 23:59, \n'
                              'если не указана дата, то дата бедет сегодняшняя. \n'
                              'Если не указаны ни время, ни дата, то отсчет будет от текущего момента. \n'
                              'Если не указан период, то точки будут отбираться за 24 часа. \n'
                              'Если не указаны регионы, будут отобраны точки по ЦР. \n'
                              'Если нужны точки по всей Росии, то указываем (\'Россия\') \n'
                              'Порядок параметров несущественен, скобки и кавычки - необходимы. \n'
                              'Просто запрос /get_data без параметров вернет все точки по ЦР за последние 24 часа.')

def help_get_around(bot, update):
    """Send a message when the command /help_get_around is issued."""
    update.message.reply_text('Строка запроса термоточек в радиусе от заданной выглядит так: \n'
                              '/get_around c120 10-19 18:00 36h (55.66173821, 41.37139873, 10000) \n'
                              'Такой запрос вернет точки с критичностью не ниже 120, \n'
                              'начиная с 18:00 19 октября, за 36 часов ("в прошлое"), \n'
                              'в радиусе 10 километров (10000 метров) от точки с координатами (55.66173821, 41.37139873). \n'
                              'Можно заменить букву "с" буквой "f", например - "f32," \n'
                              'это будет означать точки, попавшие в области торфяников с горимостью не ниже 32. \n'
                              'Если не указаны ни "c", ни "f", то предполагается значение "c0", \n'
                              'если не указано время, то оно будет 23:59, \n'
                              'если не указана дата, то дата бедет сегодняшняя. \n'
                              'Если не указаны ни время, ни дата, то отсчет будет от текущего момента. \n'
                              'Если не указан период, то точки будут отбираться за 24 часа. \n'
                              'Координаты точки и радиус указывать обязательно. \n'
                              'Порядок параметров несущественен, скобки и кавычки - необходимы. \n'
                              'Просто запрос /get_around (55.66173821, 41.37139873, 10000) вернет точки за последние 24 часа.')

def echo(bot, update):
     """Echo the user message."""
     update.message.reply_text("Что-то мы пока не знаем такой команды...")

def main():
    """Start the bot."""
    updater = Updater(token=TOKEN, use_context=False)
    disp = updater.dispatcher
    disp.add_handler(CommandHandler("start", start))
    disp.add_handler(CommandHandler("test", test))
    disp.add_handler(CommandHandler("t_subs_stat", t_subs_stat))
    disp.add_handler(CommandHandler("t_unsubs_stat", t_unsubs_stat))
    disp.add_handler(CommandHandler("t_subs_point", t_subs_point))
    disp.add_handler(CommandHandler("t_unsubs_point", t_unsubs_point))
    disp.add_handler(CommandHandler("activate", activate))
    disp.add_handler(CommandHandler("deactivate", deactivate))
    disp.add_handler(CommandHandler("add_email", add_email))
    disp.add_handler(CommandHandler("del_email", del_email))
    disp.add_handler(CommandHandler("show_emails", show_emails))
    disp.add_handler(CommandHandler("add_region", add_region))
    disp.add_handler(CommandHandler("del_region", del_region))
    disp.add_handler(CommandHandler("show_regions", show_regions))
    disp.add_handler(CommandHandler("list_regions", list_regions))
    disp.add_handler(CommandHandler("show_conf", show_conf))
    disp.add_handler(CommandHandler("get_data", get_data))
    disp.add_handler(CommandHandler("help_get_data", help_get_data))
    disp.add_handler(CommandHandler("get_around", get_around))
    disp.add_handler(CommandHandler("help_get_around", help_get_around))
    disp.add_handler(CommandHandler("help", help))
    disp.add_handler(MessageHandler(Filters.text, echo))
    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main()