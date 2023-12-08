import json
import logging
import sys
from collections import defaultdict

import psycopg2.extensions
from flask import Flask, Response

from queries import *

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False


@app.route('/ratings/<user>')
def ratings(user: str):
    with psycopg2.connect(host="db", database="postgres", user="postgres", password="rooter") as conn:
        conn: psycopg2.extensions.connection
        with conn.cursor() as cur:
            cur: psycopg2.extensions.cursor
            cur.execute(SELECT_DIRECTS_USER, vars=(user,))

            my_dirs = cur.fetchall()

            data = defaultdict(dict)

            for university, direct, rating, category_id, ctrl_number, form in my_dirs:
                cur.execute(SELECT_COUNT_USERS_CONSENT_ON_OTHER_DIRECTS,
                            vars={'category': category_id, 'rating': rating})
                consent_on_other = cur.fetchone()[0]

                cur.execute(SELECT_COUNT_USERS_CONSENT_ON_DIRECT, vars={'category': category_id, 'rating': rating})
                consent = cur.fetchone()[0]
                data[university][category_id] = {
                    'direct': f"({form}) {direct}",
                    'real_rating': rating - consent_on_other - consent,
                    'consent': consent,
                    'ctrl_number': ctrl_number,
                    'competition': 'inf' if ctrl_number <= consent else round(
                        (rating - consent_on_other - consent) / (ctrl_number - consent), 2)
                }
    return Response(json.dumps(data, ensure_ascii=False), content_type="application/json; charset=utf-8")


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO, stream=sys.stdout)
    app.run(host="0.0.0.0", port=80)
