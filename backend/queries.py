SELECT_GROUPS_USER = '''SELECT 
       u.name          AS "Университет",
       u.city          AS "Город",
       d.faculty       AS "Факультет",
       d.name          AS "Направление",
       d.form          AS "Форма",
       g.group_type    AS "Группа",
       g.ctrl_number   AS "Контрольные цифры приёма", 
       requests.rating AS "Рейтинг",
       g.id            AS "ID группы",
       d.id            AS "ID направления"
FROM requests
         JOIN groups g ON requests.group_id = g.id
         JOIN directs d ON d.id = g.direct
         JOIN universities u ON d.university = u.name
WHERE requests."user" = %(usr)s;
'''  # TODO вернуть "AND requests.year = extract(year FROM current_date)"

SELECT_COUNT_USERS_CONSENT_ON_DIRECT = '''
SELECT count(DISTINCT r.rating)
FROM groups
         JOIN requests r ON groups.id = r.group_id
WHERE groups.id = %(groupid)s
  AND r.rating < (SELECT rating FROM requests WHERE "user" = %(usr)s AND requests.group_id = %(groupid)s)
  AND r.original_doc = true;
'''  # TODO вернуть "AND r.year = extract(year FROM current_date)"

SELECT_COUNT_USERS_CONSENT_ON_OTHER_DIRECTS = '''SELECT count(DISTINCT requests.rating)
FROM requests
WHERE requests.group_id != %(groupid)s
  AND requests.original_doc = TRUE
  AND requests."user" IN (
    SELECT DISTINCT r."user"
    FROM requests AS r
    WHERE r.group_id = %(groupid)s 
        AND r.rating < (SELECT rating FROM requests WHERE "user" = %(usr)s AND requests.group_id = %(groupid)s)
);
'''

SELECT_CURRENT_PASSING_SCORE = '''SELECT min(total_sum)
FROM requests
WHERE group_id = (
    SELECT id FROM groups 
        WHERE direct = %(direct)s AND group_type = %(group)s AND category_type = %(category)s
)
LIMIT (
    SELECT ctrl_number FROM groups 
        WHERE direct = %(direct)s AND group_type = %(group)s AND category_type = %(category)s
);
'''

SELECT_CONSENT_USERS = '''SELECT total_sum
FROM requests
JOIN groups g on g.id = requests.group_id
JOIN directs d on d.id = g.direct
WHERE d.id = %(direct)s and g.group_type = %(group)s and g.category_type = %(category)s
ORDER BY total_sum DESC;
'''

SELECT_CTRL_NUMBER = '''
SELECT ctrl_number
FROM groups
JOIN directs d on d.id = direct
WHERE d.id = %(direct)s and group_type = %(group)s and category_type = %(category)s
'''