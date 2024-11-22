from collections import defaultdict

import psycopg2.extensions

from queries import *


def calculate_statistic_direct(rating: int, consent: int, consent_on_other: int, ctrl_number: int):
    return {
        'real_rating': rating - consent_on_other - consent,
        'consent': consent,
        'ctrl_number': ctrl_number,
        'competition': 'inf' if ctrl_number <= consent else round(
            (rating - consent_on_other - consent) / (ctrl_number - consent), 2)
    }


def get_user_real_rating(user: str, cur: psycopg2.extensions.cursor) -> dict:
    data = defaultdict(dict)

    for group in get_user_groups(user, cur):
        cur.execute(SELECT_COUNT_USERS_CONSENT_ON_OTHER_DIRECTS,
                    vars={'groupid': group["group_id"], 'usr': user})
        consent_on_other = cur.fetchone()[0]

        cur.execute(SELECT_COUNT_USERS_CONSENT_ON_DIRECT, vars={'groupid': group["group_id"], 'usr': user})
        consent = cur.fetchone()[0]
        stats = calculate_statistic_direct(group["rating"], consent, consent_on_other, group["ctrl_number"])
        data[group["university"]][group["group_id"]] = {**group, **stats}
    return data


def get_user_groups(user: str, cur: psycopg2.extensions.cursor) -> list[dict]:
    cur.execute(SELECT_GROUPS_USER, vars={"usr": user})
    my_groups = cur.fetchall()

    return [{
        "name": f"{faculty} ({form} {group_type}) {name}",
        "university": f"{university} (г. {city})",
        "ctrl_number": ctrl_number,
        "rating": rating,
        "group_id": group_id,
        "direct_id": direct_id
    } for university, city, faculty, name, form, group_type, ctrl_number, rating, group_id, direct_id in my_groups]


def get_current_passing_score(direct: int, group: str, category: str, cur: psycopg2.extensions.cursor) -> int:

    cur.execute(SELECT_CONSENT_USERS, vars={"direct": direct, "group": group, "category": category})
    users = cur.fetchall()

    cur.execute(SELECT_CTRL_NUMBER, vars={"direct": direct, "group": group, "category": category})
    ctrl_number = cur.fetchone()[0]
    if len(users) >= ctrl_number:
        return users[ctrl_number - 1][0]
    return -1
