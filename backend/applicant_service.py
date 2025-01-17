from collections import defaultdict
from applicant_repository import select_groups_user, select_consent_users, select_count_users_consent_on_direct, select_count_user_consent_on_other_directs, select_ctrl_number
import psycopg2.extensions


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
        consent_on_other = select_count_user_consent_on_other_directs(user, group["group_id"], cur)
        consent = select_count_users_consent_on_direct(user, group["group_id"], cur)
        stats = calculate_statistic_direct(group["rating"], consent, consent_on_other, group["ctrl_number"])
        data[group["university"]][group["group_id"]] = {**group, **stats}
    return data


def get_user_groups(user: str, cur: psycopg2.extensions.cursor) -> list[dict]:
    my_groups = select_groups_user(user, cur)

    return [{
        "name": f"{faculty} ({form} {group_type}) {name}",
        "university": f"{university} (г. {city})",
        "ctrl_number": ctrl_number,
        "rating": rating,
        "group_id": group_id,
        "direct_id": direct_id
    } for university, city, faculty, name, form, group_type, ctrl_number, rating, group_id, direct_id in my_groups]


def get_current_passing_score(direct: int, group: str, category: str, cur: psycopg2.extensions.cursor) -> int:
    users = select_consent_users(direct, group, category, cur)
    ctrl_number = select_ctrl_number(direct, group, category, cur)
    if len(users) >= ctrl_number:
        return users[ctrl_number - 1][0]
    return -1
