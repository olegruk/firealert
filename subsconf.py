"""
Module of firealert robot.

Contains functions for config subscribers.

Created:     15.10.2020

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/

"""

from faservice import get_config, get_cursor, close_conn

[subs_table] = get_config("tables", ["subs_tab"])
[conf_list, conf_desc] = get_config("subs", ["conf_list", "conf_desc"])


def add_tlg_user(telegram_id):
    """Add a new subscriber with telegram_id."""
    conn, cursor = get_cursor()
    cursor.execute(f"""SELECT subs_id
                       FROM {subs_table}
                       WHERE tlg_id = '{telegram_id}'
                    """)
    if len(cursor.fetchall()) == 0:
        cursor.execute(f"""INSERT INTO {subs_table} (tlg_id)
                                VALUES ({telegram_id})
                        """)
        res = True
    else:
        res = False
    close_conn(conn, cursor)
    return res


def set_teleg_stat(telegram_id):
    """Set attrubute for send stat to telegram."""
    conn, cursor = get_cursor()
    cursor.execute(f"""UPDATE {subs_table}
                       SET teleg_stat = TRUE
                       WHERE tlg_id = '{telegram_id}'
                    """)
    close_conn(conn, cursor)


def unset_teleg_stat(telegram_id):
    """Unset attrubute for send stat to telegram."""
    conn, cursor = get_cursor()
    cursor.execute(f"""UPDATE {subs_table}
                       SET teleg_stat = FALSE
                       WHERE tlg_id = '{telegram_id}'
                    """)
    close_conn(conn, cursor)


def set_teleg_point(telegram_id):
    """Set attrubute for send points to telegram."""
    conn, cursor = get_cursor()
    cursor.execute(f"""UPDATE {subs_table}
                       SET teleg_point = TRUE
                       WHERE tlg_id = '{telegram_id}'
                    """)
    close_conn(conn, cursor)


def unset_teleg_point(telegram_id):
    """Unset attrubute for send points to telegram."""
    conn, cursor = get_cursor()
    cursor.execute(f"""UPDATE {subs_table}
                       SET teleg_point = FALSE
                       WHERE tlg_id = '{telegram_id}'
                    """)
    close_conn(conn, cursor)


def set_active(telegram_id):
    """Set attrubute for activate user subscription."""
    conn, cursor = get_cursor()
    cursor.execute(f"""UPDATE {subs_table}
                       SET active = TRUE
                       WHERE tlg_id = '{telegram_id}'
                    """)
    close_conn(conn, cursor)


def unset_active(telegram_id):
    """Unset attrubute for activate user subscription."""
    conn, cursor = get_cursor()
    cursor.execute(f"""UPDATE {subs_table}
                       SET active = FALSE
                       WHERE tlg_id = '{telegram_id}'
                    """)
    close_conn(conn, cursor)


def add_new_email(telegram_id, email):
    """Add new email for subscriber with telegram_id."""
    conn, cursor = get_cursor()
    cursor.execute(f"""UPDATE {subs_table}
                       SET emails = emails || ', {email}'
                       WHERE
                            emails IS NOT NULL
                            AND tlg_id = '{telegram_id}'
                    """)
    cursor.execute(f"""UPDATE {subs_table}
                       SET emails = '{email}'
                       WHERE
                            emails IS NULL
                            AND tlg_id = '{telegram_id}'
                    """)
    cursor.execute(f"""SELECT emails
                       FROM {subs_table}
                       WHERE tlg_id = '{telegram_id}'
                    """)
    maillist = cursor.fetchone()
    close_conn(conn, cursor)
    return maillist


def remove_email(telegram_id, email):
    """Remove email for subscriber with telegram_id."""
    conn, cursor = get_cursor()
    cursor.execute(f"""UPDATE {subs_table}
                       SET emails = replace(emails, '{email}, ', '')
                       WHERE tlg_id = '{telegram_id}'
                    """)
    cursor.execute(f"""UPDATE {subs_table}
                       SET emails = replace(emails, ', {email}', '')
                       WHERE tlg_id = '{telegram_id}'
                    """)
    cursor.execute(f"""UPDATE {subs_table}
                       SET emails = replace(emails, '{email}', '')
                       WHERE tlg_id = '{telegram_id}'
                    """)
    cursor.execute(f"""UPDATE {subs_table}
                       SET emails = NULL
                       WHERE
                            emails = ''
                            AND tlg_id = '{telegram_id}'
                    """)
    cursor.execute(f"""SELECT emails
                       FROM {subs_table}
                       WHERE tlg_id = '{telegram_id}'
                    """)
    maillist = cursor.fetchone()
    close_conn(conn, cursor)
    return maillist


def show_maillist(telegram_id):
    """Return list of emails for subscriber with telegram_id."""
    conn, cursor = get_cursor()
    cursor.execute(f"""SELECT emails
                       FROM {subs_table}
                       WHERE tlg_id = '{telegram_id}'
                    """)
    maillist = cursor.fetchone()
    close_conn(conn, cursor)
    return maillist


def add_new_region(telegram_id, region):
    """Add new monitored region for subscriber with telegram_id."""
    conn, cursor = get_cursor()
    cursor.execute(f"""UPDATE {subs_table}
                       SET regions = replace(regions, ')', ', ''{region}'')')
                       WHERE regions IS NOT NULL
                       AND tlg_id = '{telegram_id}'
                    """)
    cursor.execute(f"""UPDATE {subs_table}
                       SET regions = '(''{region}'')'
                       WHERE regions IS NULL
                       AND tlg_id = '{telegram_id}'
                    """)
    cursor.execute(f"""SELECT regions
                       FROM {subs_table}
                       WHERE tlg_id = '{telegram_id}'
                    """)
    reglist = cursor.fetchone()
    close_conn(conn, cursor)
    return reglist


def remove_region(telegram_id, region):
    """Remove region from list of monitored for subscriber with telegram_id."""
    conn, cursor = get_cursor()
    cursor.execute(f"""UPDATE {subs_table}
                       SET regions = replace(regions, '''{region}'', ', '')
                       WHERE tlg_id = '{telegram_id}'
                    """)
    cursor.execute(f"""UPDATE {subs_table}
                       SET regions = replace(regions, ', ''{region}''', '')
                       WHERE tlg_id = '{telegram_id}'
                    """)
    cursor.execute(f"""UPDATE {subs_table}
                       SET regions = replace(regions, '''{region}''', '')
                       WHERE tlg_id = '{telegram_id}'
                    """)
    cursor.execute(f"""UPDATE {subs_table}
                       SET regions = NULL
                       WHERE regions = '()'
                       AND tlg_id = '{telegram_id}'
                    """)
    cursor.execute(f"""SELECT regions
                       FROM {subs_table}
                       WHERE tlg_id = '{telegram_id}'
                    """)
    reglist = cursor.fetchone()
    close_conn(conn, cursor)
    return reglist


def show_reglist(telegram_id):
    """Return list of monitored regions for subscriber with telegram_id."""
    conn, cursor = get_cursor()
    cursor.execute(f"""SELECT regions
                       FROM {subs_table}
                       WHERE tlg_id = '{telegram_id}'
                    """)
    reglist = cursor.fetchone()
    close_conn(conn, cursor)
    return reglist


def list_reglist():
    """Return full list of regions."""
    conn, cursor = get_cursor()
    cursor.execute("""SELECT DISTINCT ON (region) region
                      FROM monitored_area
                    """)
    reglist = cursor.fetchall()
    close_conn(conn, cursor)
    msg = ""
    for elem in reglist[0:-1]:
        msg += f"{str(elem)[2:-3]}\n"
    return msg, reglist


def show_conf(telegram_id):
    """Return current conf of subscriber."""
    conn, cursor = get_cursor()
    cursor.execute(f"""SELECT *
                       FROM {subs_table}
                       WHERE tlg_id = '{telegram_id}'
                    """)
    conf = cursor.fetchone()
    close_conn(conn, cursor)
    return conf
