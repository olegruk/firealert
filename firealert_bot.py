# -*- coding: utf-8 -*-

from telegram import KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackQueryHandler,
    ConversationHandler,
    CallbackContext)

import subsconf, requester
from faservice import get_config, get_path, send_doc_to_telegram, str_to_lst
from falogging import log, start_logging, stop_logging
import os, time, re

#from config import url, TOKEN
#[url, TOKEN] = get_config('telegramm', ['url', 'token'])
from config import url, TOKEN

# Stages
[MAIN_MENU,
SUBS_MENU,
CONF_MENU,
EMAIL_MENU,
REG_MENU,
GET_MENU,
WAIT_MAIL_TO_ADD, 
WAIT_MAIL_TO_DEL, 
WAIT_REG_TO_ADD, 
WAIT_REG_TO_DEL, 
WAIT_R_CF_ANSW, 
WAIT_R_CF_LIM, 
WAIT_R_DATE, 
WAIT_R_TIME, 
WAIT_R_PERIOD,
WAIT_R_REGLIST, 
WAIT_C_CF_ANSW, 
WAIT_C_CF_LIM, 
WAIT_C_DATE, 
WAIT_C_TIME, 
WAIT_C_PERIOD,
WAIT_C_CENTER, 
WAIT_C_RADIUS
] = range(23)
# Callback data
[MM_SUBSCRIBE,
MM_GET_POINTS,
MM_CONFIG,
MM_HELP,
MM_END,
SM_SUBS_STAT,
SM_UNS_STAT,
SM_SUBS_POINT,
SM_UNS_POINT,
CM_EMAILS,
CM_REGIONS,
CM_SHOW,
EM_ADD,
EM_DEL,
EM_SHOW,
RM_ADD,
RM_DEL,
RM_SHOW,
RM_LIST,
GM_FOR_REGLIST,
GM_FOR_RADIUS,
GM_HELP,
MM_BACK,
CM_BACK,
CF_CRITICAL,
CF_FIRELIMIT] = range(26)

"""
mm_keyboard = [
    [InlineKeyboardButton("Subscribe", callback_data=str(MM_SUBSCRIBE))],
    [InlineKeyboardButton("Get points", callback_data=str(MM_GET_POINTS))],
    [InlineKeyboardButton("Config", callback_data=str(MM_CONFIG))],
    [InlineKeyboardButton("Help", callback_data=str(MM_HELP))],
    [InlineKeyboardButton("End", callback_data=str(MM_END))]
]

sm_keyboard = [
    [InlineKeyboardButton("Subs stat on telegram", callback_data=str(SM_SUBS_STAT))],
    [InlineKeyboardButton("Unsubs stat on telegram", callback_data=str(SM_UNS_STAT))],
    [InlineKeyboardButton("Subs points on telegram", callback_data=str(SM_SUBS_POINT))],
    [InlineKeyboardButton("Unsubs points on telegram", callback_data=str(SM_UNS_POINT))],
    [InlineKeyboardButton("Go back", callback_data=str(MM_BACK))]
]

gm_keyboard = [
    [InlineKeyboardButton("Get points by region", callback_data=str(GM_FOR_REGLIST))],
    [InlineKeyboardButton("Get points by ring", callback_data=str(GM_FOR_RADIUS))],
    [InlineKeyboardButton("Get points help", callback_data=str(GM_HELP))],
    [InlineKeyboardButton("Go back", callback_data=str(MM_BACK))]
]

cm_keyboard = [
    [InlineKeyboardButton("Add or remove E-mails", callback_data=str(CM_EMAILS))],
    [InlineKeyboardButton("Add or remove regions", callback_data=str(CM_REGIONS))],
    [InlineKeyboardButton("Show current config", callback_data=str(CM_SHOW))],
    [InlineKeyboardButton("Go back", callback_data=str(MM_BACK))]
]

end_keyboard = [
    [InlineKeyboardButton("End of session", callback_data=str(MM_END))]
]

em_keyboard = [
    [InlineKeyboardButton("Add email to list", callback_data=str(EM_ADD))],
    [InlineKeyboardButton("Del email from list", callback_data=str(EM_DEL))],
    [InlineKeyboardButton("Show your emails", callback_data=str(EM_SHOW))],
    [InlineKeyboardButton("Go back", callback_data=str(CM_BACK))]
]

rm_keyboard = [
    [InlineKeyboardButton("Add region to list", callback_data=str(RM_ADD))],
    [InlineKeyboardButton("Del region from list", callback_data=str(RM_DEL))],
    [InlineKeyboardButton("Show your regions", callback_data=str(RM_SHOW))],
#    [InlineKeyboardButton("Show list of all regions", callback_data=str(RM_LIST))],
    [InlineKeyboardButton("Go back", callback_data=str(CM_BACK))]
]
"""

mm_keyboard = [
    [InlineKeyboardButton("Включить/выключить подписку", callback_data=str(MM_SUBSCRIBE))],
    [InlineKeyboardButton("Настройка подписки", callback_data=str(MM_CONFIG))],
    [InlineKeyboardButton("Получить точки прямщас!", callback_data=str(MM_GET_POINTS))],
    [InlineKeyboardButton("Завершить работу", callback_data=str(MM_END))]
]

sm_keyboard = [
    [InlineKeyboardButton("Подписаться на статистику", callback_data=str(SM_SUBS_STAT))],
    [InlineKeyboardButton("Отказаться от статистики", callback_data=str(SM_UNS_STAT))],
    [InlineKeyboardButton("Подписаться на точки", callback_data=str(SM_SUBS_POINT))],
    [InlineKeyboardButton("Больше никаких точек!", callback_data=str(SM_UNS_POINT))],
    [InlineKeyboardButton("Обратно в главное меню", callback_data=str(MM_BACK))]
]

gm_keyboard = [
    [InlineKeyboardButton("Дайте точки по региону", callback_data=str(GM_FOR_REGLIST))],
    [InlineKeyboardButton("Дайте точки вокруг точки", callback_data=str(GM_FOR_RADIUS))],
    [InlineKeyboardButton("Обратно в главное меню", callback_data=str(MM_BACK))]
]

cm_keyboard = [
    [InlineKeyboardButton("Добавить/удалить адрес", callback_data=str(CM_EMAILS))],
    [InlineKeyboardButton("Добавить/удалить регион", callback_data=str(CM_REGIONS))],
    [InlineKeyboardButton("Показать нынешние настройки", callback_data=str(CM_SHOW))],
    [InlineKeyboardButton("Обратно в главное меню", callback_data=str(MM_BACK))]
]

end_keyboard = [
    [InlineKeyboardButton("Завершаем работу!", callback_data=str(MM_END))]
]

em_keyboard = [
    [InlineKeyboardButton("Добавить адрес для рассылки", callback_data=str(EM_ADD))],
    [InlineKeyboardButton("Удалить адрес из рассылки", callback_data=str(EM_DEL))],
    [InlineKeyboardButton("Покажите адреса в моей подписке", callback_data=str(EM_SHOW))],
    [InlineKeyboardButton("Обратно в меню настройки", callback_data=str(CM_BACK))]
]

rm_keyboard = [
    [InlineKeyboardButton("Добавить регион в список", callback_data=str(RM_ADD))],
    [InlineKeyboardButton("Удалить регион", callback_data=str(RM_DEL))],
    [InlineKeyboardButton("Покажите список регионов", callback_data=str(RM_SHOW))],
#    [InlineKeyboardButton("Show list of all regions", callback_data=str(RM_LIST))],
    [InlineKeyboardButton("Обратно в меню настройки", callback_data=str(CM_BACK))]
]

cf_keyboard = [
    [InlineKeyboardButton("По критичности.", callback_data=str(CF_CRITICAL))],
    [InlineKeyboardButton("По горимости.", callback_data=str(CF_FIRELIMIT))]
]

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

    dat = re.search(r'((0[1-9]|[12]\d|3[01])\.(0[13578]|1[02]))|((0[1-9]|[12]\d|30)\.(0[13456789]|1[012]))|((0[1-9]|1\d|2[0-8])\.02)', req)
    tme = re.search(r'(([01][0-9])|(2[0-3]))\:([0-5][0-9])', req)
    if dat and tme:
        currtime = time.localtime()
        curryear = time.strftime('%Y',currtime)
        currdat = '%(m)s-%(d)s'%{'m':dat[0][3:5],'d':dat[0][0:2]}
        req_params['from_time'] = curryear + '-' + currdat + ' ' + tme[0]
    if dat and not(tme):
        currtime = time.localtime()
        curryear = time.strftime('%Y',currtime)
        currdat = '%(m)s-%(d)s'%{'m':dat[0][3:4],'d':dat[0][0:1]}
        req_params['from_time'] = curryear + '-' + currdat + ' ' + '23:59'
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

    y = re.search(r'\((N|S)\d{1,2}\.\d{1,8},', req)
    circle = ['','','']
    if y:
        circle[1] = y[0][2:-1]
        x = re.search(r', (E|W)\d{1,2}\.\d{1,8},', req)
        if x:
            circle[0] = x[0][3:-1]
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

def init(update: Update, context: CallbackContext):
    main_keyboard = [[KeyboardButton('/start'), KeyboardButton('/help')]]
    reply_kb_markup = ReplyKeyboardMarkup(main_keyboard, resize_keyboard=True, one_time_keyboard=True)
    user = update.message.from_user
    log("User %s started the conversation." %user.first_name)
    telegram_id = user.id
    subsconf.add_tlg_user(telegram_id)
    update.message.reply_text('Если ничего не понятно, скажите "/help".', reply_markup=reply_kb_markup)

def start(update: Update, context: CallbackContext) -> None:
    """Send message on `/start`."""
    # Get user that sent /start and log his name
    user = update.message.from_user
    log("User %s started the conversation." %user.first_name)
    telegram_id = user.id
    subsconf.add_tlg_user(telegram_id)
    reply_markup = InlineKeyboardMarkup(mm_keyboard)
    # Send message with text and appended InlineKeyboard
    update.message.reply_text("Самые главные кнопки:", reply_markup=reply_markup)
    # Tell ConversationHandler that we're in state `FIRST` now
    return MAIN_MENU

def start_over(update: Update, context: CallbackContext) -> None:
    """Prompt same text & keyboard as `start` does but not as new message"""
    # Get CallbackQuery from Update
    query = update.callback_query
    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    query.answer()
    reply_markup = InlineKeyboardMarkup(mm_keyboard)
    # Instead of sending a new message, edit the message that
    # originated the CallbackQuery. This gives the feeling of an
    # interactive menu.
    query.edit_message_text(text="Самые главные кнопки:", reply_markup=reply_markup)
    return MAIN_MENU

def mm_subscribe(update: Update, context: CallbackContext) -> None:
    """Show new choice of buttons"""
    query = update.callback_query
    query.answer()
    reply_markup = InlineKeyboardMarkup(sm_keyboard)
    query.edit_message_text(
        text="Тут можно включить подписку. Или выключить", reply_markup=reply_markup
    )
    return SUBS_MENU

def mm_get_points(update: Update, context: CallbackContext) -> None:
    """Show new choice of buttons"""
    query = update.callback_query
    query.answer()
    reply_markup = InlineKeyboardMarkup(gm_keyboard)
    query.edit_message_text(
        text="Тут можно получить точки прямщас.", reply_markup=reply_markup
    )
    return GET_MENU

def mm_config(update: Update, context: CallbackContext) -> None:
    """Show new choice of buttons"""
    query = update.callback_query
    query.answer()
    reply_markup = InlineKeyboardMarkup(cm_keyboard)
    query.edit_message_text(
        text="Тут можно настроить свою подписку.", reply_markup=reply_markup
    )
    return CONF_MENU

def sm_subs_stat(update: Update, context: CallbackContext) -> None:
    """Subscribe for statistic on telegram when the command /t_subs_stat is issued."""
    query = update.callback_query
    query.answer()
    telegram_id = query.from_user.id
    subsconf.set_teleg_stat(telegram_id)
    reply_markup = InlineKeyboardMarkup(sm_keyboard)
    query.edit_message_text(
        text='Вы подписаны на рассылку статистики в телеграм.', reply_markup=reply_markup)
    return SUBS_MENU

def sm_uns_stat(update: Update, context: CallbackContext) -> None:
    """Unsubscribe statistic on telegram when the command /t_unsubs_stat is issued."""
    query = update.callback_query
    query.answer()
    telegram_id = query.from_user.id
    subsconf.unset_teleg_stat(telegram_id)
    reply_markup = InlineKeyboardMarkup(sm_keyboard)
    query.edit_message_text(
        text='Вы отписаны от рассылки статистики в телеграм.', reply_markup=reply_markup)
    return SUBS_MENU

def sm_subs_point(update: Update, context: CallbackContext) -> None:
    """Subscribe for firepoints on telegram when the command /t_subs_point is issued."""
    query = update.callback_query
    query.answer()
    telegram_id = query.from_user.id
    subsconf.set_teleg_point(telegram_id)
    reply_markup = InlineKeyboardMarkup(sm_keyboard)
    query.edit_message_text(
        text='Вы подписаны на рассылку термоточек в телеграм.', reply_markup=reply_markup)
    return SUBS_MENU

def sm_uns_point(update: Update, context: CallbackContext) -> None:
    """Unsubscribe firepoints on telegram when the command /t_unsubs_point is issued."""
    query = update.callback_query
    query.answer()
    telegram_id = query.from_user.id
    subsconf.unset_teleg_point(telegram_id)
    reply_markup = InlineKeyboardMarkup(sm_keyboard)
    query.edit_message_text(
        text='Вы отписаны от рассылки термоточек в телеграм.', reply_markup=reply_markup)
    return SUBS_MENU

def cm_emails(update: Update, context: CallbackContext) -> None:
    """Show new choice of buttons"""
    query = update.callback_query
    query.answer()
    reply_markup = InlineKeyboardMarkup(em_keyboard)
    query.edit_message_text(
        text="Тут можно добавить или удалить из подписки почтовые адреса.", reply_markup=reply_markup
    )
    return EMAIL_MENU

def cm_regions(update: Update, context: CallbackContext) -> None:
    """Show new choice of buttons"""
    query = update.callback_query
    query.answer()
    reply_markup = InlineKeyboardMarkup(rm_keyboard)
    query.edit_message_text(
        text="Добавить или исключить из подписки регионы.", reply_markup=reply_markup
    )
    return REG_MENU

def cm_show(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    telegram_id = query.from_user.id
    conf = subsconf.show_conf(telegram_id)
    paramstr = ''
    for i in range(len(subsconf.conf_desc)):
        paramstr = paramstr + '\n' + '%(s)s %(p)s' %{'s':subsconf.conf_desc[i],'p':conf[i]}
        #paramstr = paramstr + '\n' + '%(s)s\n%(c)s%(p)s' %{'s':subsconf.conf_desc[i],'c':command_list[i],'p':conf[i]}
        #update.message.reply_text(paramstr)
    reply_markup = InlineKeyboardMarkup(cm_keyboard)
    query.edit_message_text(text=paramstr, reply_markup=reply_markup)
    return CONF_MENU

def em_add(update: Update, context: CallbackContext) -> None:
    """Show new choice of buttons"""
    query = update.callback_query
    query.answer()
    query.edit_message_text(
        text="Пишите адрес для добавления в подписку.")
    return WAIT_MAIL_TO_ADD

def add_email(update: Update, context: CallbackContext):
    telegram_id = update.message.from_user.id
    email = update.message.text
    update.message.reply_text('Добавляем адрес: %s'%email)
    maillist = subsconf.add_new_email(telegram_id, email)
    reply_markup = InlineKeyboardMarkup(em_keyboard)
    update.message.reply_text('Список адресов обновлен:\n%s' %maillist, reply_markup=reply_markup)
    return EMAIL_MENU

def em_del(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    telegram_id = query.from_user.id
    maillist = str(subsconf.show_maillist(telegram_id))[2:-3].split(', ')
    msg = ''
    for num, elem in enumerate(maillist):
        msg = msg + str(num+1) + ' ' + elem + '\n'
    query.edit_message_text(
        text='Список адресов в подписке:\n%s\n'
        'Укажите номер для удаления.\n'%msg)
    return WAIT_MAIL_TO_DEL

def del_email(update: Update, context: CallbackContext):
    telegram_id = update.message.from_user.id
    message = update.message.text
    maillist = str(subsconf.show_maillist(telegram_id))[2:-3].split(', ')
    email = maillist[int(message)-1]
    update.message.reply_text('Удаляем адрес: %s'%email)
    maillist = subsconf.remove_email(telegram_id, email)
    reply_markup = InlineKeyboardMarkup(em_keyboard)
    update.message.reply_text('Список адресов обновлен:\n%s' %maillist, reply_markup=reply_markup)
    return EMAIL_MENU

def em_show(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    telegram_id = query.from_user.id
    maillist = subsconf.show_maillist(telegram_id)
    reply_markup = InlineKeyboardMarkup(em_keyboard)
    query.edit_message_text(text='Список рассылки:\n%s' %maillist, reply_markup=reply_markup)
    return EMAIL_MENU

def rm_add(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    reglist = subsconf.list_reglist()
    msg = '0 Вся Россия\n'
    for num, elem in enumerate(reglist[0:-1]):
        msg = msg + str(num+1) + ' ' + str(elem)[2:-3] + '\n'

    query.edit_message_text(
        text='Список всех регионов:\n%s\n'
        'Укажите номер региона для добавления.\n'
        'Можно указать несколько номеров через запятую.'%msg)
    return WAIT_REG_TO_ADD

def add_region1(update: Update, context: CallbackContext):
    telegram_id = update.message.from_user.id
    message = update.message.text
    reglist = subsconf.list_reglist()
    if message =='0':
        region = 'Россия'
    else:
        region = str(reglist[0:-1][int(message)-1])[2:-3]
    update.message.reply_text('Добавляем регион: %s'%region)
    reglist = subsconf.add_new_region(telegram_id, region)
    reply_markup = InlineKeyboardMarkup(rm_keyboard)
    update.message.reply_text('Список регионов обновлен:\n%s'%reglist, reply_markup=reply_markup)
    return REG_MENU

def add_region(update: Update, context: CallbackContext):
    telegram_id = update.message.from_user.id
    message = update.message.text
    reglist = subsconf.list_reglist()
    if message =='0':
        region = 'Россия'
    else:
        numlist = message.strip().split(',')
        for num in numlist:
            region = str(reglist[0:-1][int(num)-1])[2:-3]
            update.message.reply_text('Добавляем регион: %s'%region)
            final_reglist = subsconf.add_new_region(telegram_id, region)
    reply_markup = InlineKeyboardMarkup(rm_keyboard)
    update.message.reply_text('Список регионов обновлен:\n%s'%final_reglist, reply_markup=reply_markup)
    return REG_MENU

def rm_del(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    telegram_id = query.from_user.id
    reglist = str_to_lst(str(subsconf.show_reglist(telegram_id))[3:-4])
    msg = ''
    for num, elem in enumerate(reglist):
        msg = msg + str(num+1) + ' ' + elem + '\n'
    query.edit_message_text(
        text='Список регионов в подписке:\n%s\n'
        'Укажите номер региона для удаления.\n'
        'Можно указать несколько номеров через запятую.'%msg)
    return WAIT_REG_TO_DEL

def delete_region(update: Update, context: CallbackContext):
    telegram_id = update.message.from_user.id
    message = update.message.text
    reglist = str_to_lst(str(subsconf.show_reglist(telegram_id))[3:-4])
    numlist = message.strip().split(',')
    for num in numlist:
        region = reglist[int(num)-1]
        update.message.reply_text('Удаляем регион: %s'%region)
        final_reglist = subsconf.remove_region(telegram_id, region)
    reply_markup = InlineKeyboardMarkup(rm_keyboard)
    update.message.reply_text('Список регионов обновлен:\n%s'%final_reglist, reply_markup=reply_markup)
    return REG_MENU

def rm_show(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    telegram_id = query.from_user.id
    reglist = subsconf.show_reglist(telegram_id)
    reply_markup = InlineKeyboardMarkup(rm_keyboard)
    query.edit_message_text(text='Список регионов в подписке:\n%s' %reglist, reply_markup=reply_markup)
    return REG_MENU

def rm_list(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    reglist = subsconf.list_reglist()
    reply_markup = InlineKeyboardMarkup(rm_keyboard)
    query.edit_message_text(text='Список возможных регионов:\n%s' %reglist, reply_markup=reply_markup)
    return REG_MENU

def gm_for_reglist(update: Update, context: CallbackContext) -> None:
    """Show new choice of buttons"""
    query = update.callback_query
    query.answer()
    reply_markup = InlineKeyboardMarkup(cf_keyboard)
    query.edit_message_text(
        text="Отбираем точки по критичности или по горимости торфяника?", reply_markup=reply_markup
    )
    return WAIT_R_CF_ANSW

def get_r_cf_answ_c(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    context.user_data['request'] = '/get_data c'
    query.edit_message_text(text='Задайте порог критичности.\nБудут отобраны все точки с критичностью больше либо равной этому порогу.')
    return WAIT_R_CF_LIM

def get_r_cf_answ_f(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    context.user_data['request'] = '/get_data f'
    query.edit_message_text(text='Задайте порог горимости.\nБудут отобраны все точки с горимостью больше либо равной этому порогу.')
    return WAIT_R_CF_LIM

def get_r_cf_lim(update: Update, context: CallbackContext):
    message = update.message.text
    context.user_data['request'] = context.user_data['request'] + message
    update.message.reply_text('Задайте дату (в формате ДД.ММ) до которой будут отбираться точки.')
    return WAIT_R_DATE

def get_r_date(update: Update, context: CallbackContext):
    message = update.message.text
    context.user_data['request'] = context.user_data['request'] + ' ' + message
    update.message.reply_text('И время (в формате ЧЧ:ММ).')
    return WAIT_R_TIME

def get_r_time(update: Update, context: CallbackContext):
    message = update.message.text
    context.user_data['request'] = context.user_data['request'] + ' ' + message
    update.message.reply_text("Задайте период за который надо отобрать точки.\n"
                                "Возможны варианты '12h', '32 hours', '2 months' или их комбинации.")
    return WAIT_R_PERIOD

def get_r_period1(update: Update, context: CallbackContext):
    message = update.message.text
    context.user_data['request'] = context.user_data['request'] + ' ' + message + 'h'
    update.message.reply_text('Укажите регион, по которому будут отобраны точки.')
    return WAIT_R_REGLIST

def get_r_period(update: Update, context: CallbackContext):
    message = update.message.text
    context.user_data['request'] = context.user_data['request'] + ' ' + message + 'h'
    reglist = subsconf.list_reglist()
    msg = '0 Вся Россия\n'
    for num, elem in enumerate(reglist[0:-1]):
        msg = msg + str(num+1) + ' ' + str(elem)[2:-3] + '\n'
    update.message.reply_text('Список всех регионов:\n%s\n'
                                'Укажите номер региона, по которому будут отобраны точки.\n'
                                'Можно указать несколько номеров через запятую.\n'
                                'ЦР - Центральный регион.'%msg)
    return WAIT_R_REGLIST

def get_r_reglist(update: Update, context: CallbackContext):
    [data_root,temp_folder] = get_config("path", ["data_root", "temp_folder"])
    #Создаем каталог для записи временных файлов
    result_dir = get_path(data_root,temp_folder)
    telegram_id = update.message.from_user.id
    message = update.message.text
    reglist = subsconf.list_reglist()
    if message =='0':
        final_reglist = "'Россия'"
    elif message == 'ЦР':
        final_reglist = ""
    else:
        numlist = message.strip().split(',')
        final_reglist = ''
        for num in numlist[0:-1]:
            region = str(reglist[0:-1][int(num)-1])[2:-3]
            final_reglist = final_reglist + "'%s', "%region
        region = str(reglist[0:-1][int(numlist[-1])-1])[2:-3]
        final_reglist = final_reglist + "'%s'"%region
    if message != '':
        context.user_data['request'] = context.user_data['request'] + ' ' + '(' + final_reglist + ')'
    update.message.reply_text('Сейчас сюда прилетят точки.')
    req_params = parse_data_req(context.user_data['request'])
    log('Запрошен файл с точками по параметрам:\n%s'%req_params)
    dst_file, nump = requester.request_data(telegram_id, req_params['lim_for'], req_params['limit'], req_params['from_time'], req_params['period'], req_params['regions'], result_dir)
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
    reply_markup = InlineKeyboardMarkup(gm_keyboard)
    update.message.reply_text('В файле %(p)s %(t)s.' %{'p':str_nump, 't':tail}, reply_markup=reply_markup)
    return GET_MENU

def gm_for_radius(update: Update, context: CallbackContext) -> None:
    """Show new choice of buttons"""
    query = update.callback_query
    query.answer()
    reply_markup = InlineKeyboardMarkup(cf_keyboard)
    query.edit_message_text(
        text="Отбираем точки по критичности или по горимости торфяника?", reply_markup=reply_markup
    )
    return WAIT_C_CF_ANSW

def get_c_cf_answ_c(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    context.user_data['request'] = '/get_data c'
    query.edit_message_text(text='Задайте порог критичности.\nБудут отобраны все точки с критичностью больше либо равной этому порогу.')
    return WAIT_C_CF_LIM

def get_c_cf_answ_f(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    context.user_data['request'] = '/get_data f'
    query.edit_message_text(text='Задайте порог горимости.\nБудут отобраны все точки с горимостью больше либо равной этому порогу.')
    return WAIT_C_CF_LIM

def get_c_cf_lim(update: Update, context: CallbackContext):
    message = update.message.text
    context.user_data['request'] = context.user_data['request'] + message
    update.message.reply_text('Задайте дату (в формате ДД.ММ) до которой будут отбираться точки.')
    return WAIT_C_DATE

def get_c_date(update: Update, context: CallbackContext):
    message = update.message.text
    context.user_data['request'] = context.user_data['request'] + ' ' + message
    update.message.reply_text('И время (в формате ЧЧ:ММ).')
    return WAIT_C_TIME

def get_c_time(update: Update, context: CallbackContext):
    message = update.message.text
    context.user_data['request'] = context.user_data['request'] + ' ' + message
    update.message.reply_text("Задайте период за который надо отобрать точки.\n"
                                "Возможны варианты '12h', '32 hours', '2 months' или их комбинации.")
    return WAIT_C_PERIOD

def get_c_period(update: Update, context: CallbackContext):
    message = update.message.text
    context.user_data['request'] = context.user_data['request'] + ' ' + message + 'h'
    update.message.reply_text('Укажите координаты точки (в виде N55.344665, E32.455656), в окрестности которой будут отобраны точки.')
    return WAIT_C_CENTER

def get_c_center(update: Update, context: CallbackContext):
    message = update.message.text
    context.user_data['request'] = context.user_data['request'] + ' (' + message + ', '
    update.message.reply_text('Укажите радиус окрестности (в километрах), в которой будут отобраны точки.')
    return WAIT_C_RADIUS

def get_c_radius(update: Update, context: CallbackContext):
    [data_root,temp_folder] = get_config("path", ["data_root", "temp_folder"])
    #Создаем каталог для записи временных файлов
    result_dir = get_path(data_root,temp_folder)
    telegram_id = update.message.from_user.id
    message = update.message.text
    context.user_data['request'] = context.user_data['request'] + str(int(message)*1000) + ')'
    req_params = parse_data_req(context.user_data['request'])
    if req_params['circle'][1] == '':
        reply_markup = InlineKeyboardMarkup(gm_keyboard)
        update.message.reply_text('Вы неверно задали область поиска точек (скорее всего ошибка в написании координат центра).', reply_markup=reply_markup)
    else:
        update.message.reply_text('Сейчас сюда прилетят точки.')
        log('Запрошен файл с точками по параметрам:\n%s'%req_params)
        dst_file, nump = requester.request_for_circle(telegram_id, req_params['lim_for'], req_params['limit'], req_params['from_time'], req_params['period'], req_params['circle'], result_dir)
        drop_temp_files(result_dir)
        doc = open(dst_file, 'rb')
        send_doc_to_telegram(url, telegram_id, doc)
        str_nump = str(nump)
        if str_nump[-1] == '1':
            tail = 'точка'
        elif str_nump[-1] in ['2','3','4']:
            tail = 'точки'
        else:
            tail = 'точек'
        reply_markup = InlineKeyboardMarkup(gm_keyboard)
        update.message.reply_text('В файле %(p)s %(t)s.' %{'p':str_nump, 't':tail}, reply_markup=reply_markup)
    return GET_MENU

def mm_back(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    reply_markup = InlineKeyboardMarkup(mm_keyboard)
    query.edit_message_text(text="Самые главные кнопки:", reply_markup=reply_markup)
    return MAIN_MENU

def cm_back(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    reply_markup = InlineKeyboardMarkup(cm_keyboard)
    query.edit_message_text(text="Тут можно настроить свою подписку.", reply_markup=reply_markup)
    return CONF_MENU

def mm_end(update: Update, context: CallbackContext) -> None:
    """Returns `ConversationHandler.END`, which tells the
    ConversationHandler that the conversation is over"""
    query = update.callback_query
    query.answer()
    query.edit_message_text(text="Как жаль, что вы наконец-то уходите!")
    return ConversationHandler.END

def manual_t_subs_stat(update: Update, context: CallbackContext):
    """Subscribe for statistic on telegram when the command /t_subs_stat is issued."""
    telegram_id = update.message.from_user.id
    subsconf.set_teleg_stat(telegram_id)
    update.message.reply_text('Вы подписаны на рассылку статистики в телеграм.')

def manual_t_unsubs_stat(update: Update, context: CallbackContext):
    """Unsubscribe statistic on telegram when the command /t_unsubs_stat is issued."""
    telegram_id = update.message.from_user.id
    subsconf.unset_teleg_stat(telegram_id)
    update.message.reply_text('Вы отписаны от рассылки статистики в телеграм.')

def manual_t_subs_point(update: Update, context: CallbackContext):
    """Subscribe for firepoints on telegram when the command /t_subs_point is issued."""
    telegram_id = update.message.from_user.id
    subsconf.set_teleg_point(telegram_id)
    update.message.reply_text('Вы подписаны на рассылку термоточек в телеграм.')

def manual_t_unsubs_point(update: Update, context: CallbackContext):
    """Unsubscribe firepoints on telegram when the command /t_unsubs_point is issued."""
    telegram_id = update.message.from_user.id
    subsconf.unset_teleg_point(telegram_id)
    update.message.reply_text('Вы отписаны от рассылки термоточек в телеграм.')

def manual_activate(update: Update, context: CallbackContext):
    """Activate subscribtion when the command /activate is issued."""
    telegram_id = update.message.from_user.id
    subsconf.set_active(telegram_id)
    update.message.reply_text('Подписка активирована.')

def manual_deactivate(update: Update, context: CallbackContext):
    """Deactivate subscribtion when the command /deactivate is issued."""
    telegram_id = update.message.from_user.id
    subsconf.unset_active(telegram_id)
    update.message.reply_text('Подписка отключена.')

def manual_add_email(update: Update, context: CallbackContext):
    #telegram_id = update.message.from_user.id
    #message = update.message.text
    #email = message.split(' ', 1)[1]
    update.message.reply_text('Введите новый адрес.')
    #WAIT_FOR_EMAIL_TO_ADD = True
    #maillist = subsconf.add_new_email(telegram_id, email)
    #update.message.reply_text('Список адресов обновлен:\n%s' %maillist)

def manual_del_email(update: Update, context: CallbackContext):
    telegram_id = update.message.from_user.id
    message = update.message.text
    email = message.split(' ', 1)[1]
    maillist = subsconf.remove_email(telegram_id, email)
    update.message.reply_text('Список адресов обновлен:\n%s' %maillist)

def manual_show_emails(update: Update, context: CallbackContext):
    telegram_id = update.message.from_user.id
    maillist = subsconf.show_maillist(telegram_id)
    update.message.reply_text('Список рассылки:\n%s' %maillist)

def manual_add_region(update: Update, context: CallbackContext):
    telegram_id = update.message.from_user.id
    message = update.message.text
    region = message.split(' ', 1)[1]
    print(region)
    reglist = subsconf.add_new_region(telegram_id, region)
    update.message.reply_text('Список регионов обновлен:\n%s' %reglist)

def manual_del_region(update: Update, context: CallbackContext):
    telegram_id = update.message.from_user.id
    message = update.message.text
    region = message.split(' ', 1)[1]
    reglist = subsconf.remove_region(telegram_id, region)
    update.message.reply_text('Список регионов обновлен:\n%s' %reglist)

def manual_show_regions(update: Update, context: CallbackContext):
    telegram_id = update.message.from_user.id
    reglist = subsconf.show_reglist(telegram_id)
    update.message.reply_text('Список регионов:\n%s' %reglist)

def manual_list_regions(update: Update, context: CallbackContext):
    reglist = subsconf.list_reglist()
    update.message.reply_text('Список возможных регионов:\n%s' %reglist)

def manual_show_conf(update: Update, context: CallbackContext):
    telegram_id = update.message.from_user.id
    conf = subsconf.show_conf(telegram_id)
    for i in range(0,21):
        paramstr = '%(s)s\n%(c)s%(p)s' %{'s':subsconf.conf_desc[i],'c':command_list[i],'p':conf[i]}
        update.message.reply_text(paramstr)

def manual_get_data(update: Update, context: CallbackContext):
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

def manual_get_around(update: Update, context: CallbackContext):
    [data_root,temp_folder] = get_config("path", ["data_root", "temp_folder"])
    #Создаем каталог для записи временных файлов
    result_dir = get_path(data_root,temp_folder)

    telegram_id = update.message.from_user.id
    message = update.message.text

    req_params = parse_data_req(message)
    if req_params['circle'][1] == '':
        update.message.reply_text('Вы неверно задали область поиска точек (скорее всего ошибка в написании координат центра).')
    else:
        dst_file, nump = requester.request_for_circle(telegram_id, req_params['lim_for'], req_params['limit'], req_params['from_time'], req_params['period'], req_params['circle'], result_dir)
        drop_temp_files(result_dir)
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

def manual_help(update: Update, context: CallbackContext):
    """Send a message when the command /help is issued."""
    update.message.reply_text('Тут можно плдучить результат двумя путями - через диалог (с кнопками и подсказками) и через команды, набираемые в строке:\n'
                              'Команды тут такие:\n'
                              '/start - \n'
                              'начать диалог; \n'
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
                              'посмотреть, какие регионы бывают; \n'
                              '/show_conf - \n'
                              'показать параметры моей рассылки; \n'
                              '/get_data - \n'
                              'запросить точки (подробнее - /help_get_data); \n'
                              '/get_arround - \n'
                              'запросить точки в радиусе (подробнее - /help_get_around).')

def manual_help_get_data(update: Update, context: CallbackContext):
    """Send a message when the command /help_get_data is issued."""
    update.message.reply_text('Строка запроса термоточек выглядит так: \n'
                              '/get_data c120 19.10 18:00 36h (\'Московская область\', \'Иркутская область\', \'Красноярский край\') \n'
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

def manual_help_get_around(update: Update, context: CallbackContext):
    """Send a message when the command /help_get_around is issued."""
    update.message.reply_text('Строка запроса термоточек в радиусе от заданной выглядит так: \n'
                              '/get_around c120 19.10 18:00 36h (N55.66173821, E41.37139873, 10000) \n'
                              'Такой запрос вернет точки с критичностью не ниже 120, \n'
                              'начиная с 18:00 19 октября, за 36 часов ("в прошлое"), \n'
                              'в радиусе 10 километров (10000 метров) от точки с координатами (N55.66173821, E41.37139873). \n'
                              'Можно заменить букву "с" буквой "f", например - "f32," \n'
                              'это будет означать точки, попавшие в области торфяников с горимостью не ниже 32. \n'
                              'Если не указаны ни "c", ни "f", то предполагается значение "c0", \n'
                              'если не указано время, то оно будет 23:59, \n'
                              'если не указана дата, то дата бедет сегодняшняя. \n'
                              'Если не указаны ни время, ни дата, то отсчет будет от текущего момента. \n'
                              'Если не указан период, то точки будут отбираться за 24 часа. \n'
                              'Координаты точки и радиус указывать обязательно. \n'
                              'Порядок параметров несущественен, скобки и кавычки - необходимы. \n'
                              'Просто запрос /get_around (N55.66173821, E41.37139873, 10000) вернет точки за последние 24 часа.')


def main():
    """Start the bot."""
    start_logging('firealert_bot.py')
    # Create the Updater and pass it your bot's token.
    updater = Updater(TOKEN)
    # Get the dispatcher to register handlers
    disp = updater.dispatcher

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            MAIN_MENU: [
                CallbackQueryHandler(mm_subscribe, pattern='^' + str(MM_SUBSCRIBE) + '$'),
                CallbackQueryHandler(mm_get_points, pattern='^' + str(MM_GET_POINTS) + '$'),
                CallbackQueryHandler(mm_config, pattern='^' + str(MM_CONFIG) + '$'),
                CallbackQueryHandler(mm_end, pattern='^' + str(MM_END) + '$')
            ],
            SUBS_MENU: [
                CallbackQueryHandler(sm_subs_stat, pattern='^' + str(SM_SUBS_STAT) + '$'),
                CallbackQueryHandler(sm_uns_stat, pattern='^' + str(SM_UNS_STAT) + '$'),
                CallbackQueryHandler(sm_subs_point, pattern='^' + str(SM_SUBS_POINT) + '$'),
                CallbackQueryHandler(sm_uns_point, pattern='^' + str(SM_UNS_POINT) + '$'),
                CallbackQueryHandler(mm_back, pattern='^' + str(MM_BACK) + '$')
            ],
            CONF_MENU: [
                CallbackQueryHandler(cm_emails, pattern='^' + str(CM_EMAILS) + '$'),
                CallbackQueryHandler(cm_regions, pattern='^' + str(CM_REGIONS) + '$'),
                CallbackQueryHandler(cm_show, pattern='^' + str(CM_SHOW) + '$'),
                CallbackQueryHandler(mm_back, pattern='^' + str(MM_BACK) + '$')
            ],
            EMAIL_MENU: [
                CallbackQueryHandler(em_add, pattern='^' + str(EM_ADD) + '$'),
                CallbackQueryHandler(em_del, pattern='^' + str(EM_DEL) + '$'),
                CallbackQueryHandler(em_show, pattern='^' + str(EM_SHOW) + '$'),
                CallbackQueryHandler(cm_back, pattern='^' + str(CM_BACK) + '$')
            ],
            REG_MENU: [
                CallbackQueryHandler(rm_add, pattern='^' + str(RM_ADD) + '$'),
                CallbackQueryHandler(rm_del, pattern='^' + str(RM_DEL) + '$'),
                CallbackQueryHandler(rm_show, pattern='^' + str(RM_SHOW) + '$'),
                CallbackQueryHandler(rm_list, pattern='^' + str(RM_LIST) + '$'),
                CallbackQueryHandler(cm_back, pattern='^' + str(CM_BACK) + '$')
            ],
            GET_MENU: [
                CallbackQueryHandler(gm_for_reglist, pattern='^' + str(GM_FOR_REGLIST) + '$'),
                CallbackQueryHandler(gm_for_radius, pattern='^' + str(GM_FOR_RADIUS) + '$'),
                CallbackQueryHandler(mm_back, pattern='^' + str(MM_BACK) + '$')
            ],
            WAIT_MAIL_TO_ADD: [
                MessageHandler(Filters.text, add_email)
            ],
            WAIT_MAIL_TO_DEL: [
                MessageHandler(Filters.text, del_email)
            ],
            WAIT_REG_TO_ADD: [
                MessageHandler(Filters.text, add_region)
            ],
            WAIT_REG_TO_DEL: [
                MessageHandler(Filters.text, delete_region)
            ],
            WAIT_R_CF_ANSW: [
                CallbackQueryHandler(get_r_cf_answ_c, pattern='^' + str(CF_CRITICAL) + '$'),
                CallbackQueryHandler(get_r_cf_answ_f, pattern='^' + str(CF_FIRELIMIT) + '$'),
           ],
            WAIT_R_CF_LIM: [
                MessageHandler(Filters.text, get_r_cf_lim)
            ],
            WAIT_R_DATE: [
                MessageHandler(Filters.text, get_r_date)
            ],
            WAIT_R_TIME: [
                MessageHandler(Filters.text, get_r_time)
            ],
            WAIT_R_PERIOD: [
                MessageHandler(Filters.text, get_r_period)
            ],
            WAIT_R_REGLIST: [
                MessageHandler(Filters.text, get_r_reglist)
            ],
            WAIT_C_CF_ANSW: [
                CallbackQueryHandler(get_c_cf_answ_c, pattern='^' + str(CF_CRITICAL) + '$'),
                CallbackQueryHandler(get_c_cf_answ_f, pattern='^' + str(CF_FIRELIMIT) + '$'),
            ],
            WAIT_C_CF_LIM: [
                MessageHandler(Filters.text, get_c_cf_lim)
            ],
            WAIT_C_DATE: [
                MessageHandler(Filters.text, get_c_date)
            ],
            WAIT_C_TIME: [
                MessageHandler(Filters.text, get_c_time)
            ],
            WAIT_C_PERIOD: [
                MessageHandler(Filters.text, get_c_period)
            ],
            WAIT_C_CENTER: [
                MessageHandler(Filters.text, get_c_center)
            ],
            WAIT_C_RADIUS: [
                MessageHandler(Filters.text, get_c_radius)
            ]
        },
        fallbacks=[CommandHandler('start', start)]
    )

    # Add ConversationHandler to dispatcher that will be used for handling
    # updates
    disp.add_handler(conv_handler)
    disp.add_handler(CommandHandler("init", init))
    disp.add_handler(CommandHandler("t_subs_stat", manual_t_subs_stat))
    disp.add_handler(CommandHandler("t_unsubs_stat", manual_t_unsubs_stat))
    disp.add_handler(CommandHandler("t_subs_point", manual_t_subs_point))
    disp.add_handler(CommandHandler("t_unsubs_point", manual_t_unsubs_point))
    disp.add_handler(CommandHandler("activate", manual_activate))
    disp.add_handler(CommandHandler("deactivate", manual_deactivate))
    disp.add_handler(CommandHandler("add_email", manual_add_email))
    disp.add_handler(CommandHandler("del_email", manual_del_email))
    disp.add_handler(CommandHandler("show_emails", manual_show_emails))
    disp.add_handler(CommandHandler("add_region", manual_add_region))
    disp.add_handler(CommandHandler("del_region", manual_del_region))
    disp.add_handler(CommandHandler("show_regions", manual_show_regions))
    disp.add_handler(CommandHandler("list_regions", manual_list_regions))
    disp.add_handler(CommandHandler("show_conf", manual_show_conf))
    disp.add_handler(CommandHandler("get_data", manual_get_data))
    disp.add_handler(CommandHandler("help_get_data", manual_help_get_data))
    disp.add_handler(CommandHandler("get_around", manual_get_around))
    disp.add_handler(CommandHandler("help_get_around", manual_help_get_around))
    disp.add_handler(CommandHandler("help", manual_help))
    # Start the Bot
    updater.start_polling()
    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()
    stop_logging('firalert_bot.py')

if __name__ == '__main__':
    main()
