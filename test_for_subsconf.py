import subsconf
telegram_id = 580325825

def main():
    #maillist = subsconf.add_new_email(telegram_id, 'r@mail.ru')
    #subsconf.remove_email(telegram_id, 'e@mail.ru')
    #subsconf.add_new_region(telegram_id, 'Ленская область')
    #subsconf.remove_region(telegram_id, 'Брянская область')
    conf = subsconf.show_conf(telegram_id)
    for i in range(0,21):
        paramstr = '%(s)s %(p)s' %{'s':subsconf.conf_desc[i],'p':conf[i]}
        print(paramstr)


if __name__ == '__main__':
    main()
