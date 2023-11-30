SELECT_DIRECTS_USER = '''SELECT u.name, d.name, requests.rating, c.id, c.ctrl_number
FROM requests
         JOIN categories c ON c.id = requests.category
         JOIN groups g ON g.id = c."group"
         JOIN directs d ON d.id = g.direct
         JOIN faculties f ON d.faculty = f.id
         JOIN universities u ON f.university = u.name
WHERE requests."user" = %s
'''

SELECT_COUNT_USERS_CONSENT_ON_DIRECT = '''SELECT count(DISTINCT reqs.rating)
from categories
         JOIN requests reqs on categories.id = reqs.category
WHERE categories.id = %(category)s
  and reqs.original_doc = true

  and reqs.rating < %(rating)s
'''

SELECT_COUNT_USERS_CONSENT_ON_OTHER_DIRECTS = '''SELECT count(DISTINCT reqs.rating)
from categories
         JOIN requests reqs on categories.id = reqs.category
         JOIN users opponents on opponents.id = reqs."user"
         JOIN requests other_requests on other_requests."user" = opponents.id

WHERE categories.id = %(category)s
  and other_requests.original_doc = true
  and other_requests.category != %(category)s
and reqs.rating < %(rating)s'''
