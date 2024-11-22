from queries import *
from psycopg2.extensions import cursor


def select_groups_user(user: str, cur: cursor):
    cur.execute(SELECT_GROUPS_USER, vars={"usr": user})
    return cur.fetchall()


def select_count_users_consent_on_direct(user: str, group_id: int, cur: cursor):
    cur.execute(SELECT_COUNT_USERS_CONSENT_ON_DIRECT, vars={'groupid': group_id, 'usr': user})
    return cur.fetchone()[0]


def select_count_user_consent_on_other_directs(user: str, group_id: int, cur: cursor):
    cur.execute(SELECT_COUNT_USERS_CONSENT_ON_OTHER_DIRECTS,
                vars={'groupid': group_id, 'usr': user})
    return cur.fetchone()[0]


def select_current_passing_score():
    pass


def select_consent_users(direct: int, group_type: str, category_type: str, cur: cursor):
    cur.execute(SELECT_CONSENT_USERS, vars={"direct": direct, "group": group_type, "category": category_type})
    return cur.fetchall()


def select_ctrl_number(direct: int, group_type: str, category_type: str, cur: cursor):
    cur.execute(SELECT_CTRL_NUMBER, vars={"direct": direct, "group": group_type, "category": category_type})
    return cur.fetchone()[0]
