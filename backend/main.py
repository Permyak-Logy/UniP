import psycopg2.extensions
from collections import defaultdict
from queries import *
import json
from flask import Flask, jsonify

app = Flask(__name__)


@app.route('/ratings/<user>')
def ratings(user: str):
    with psycopg2.connect(host="db", database="postgres", user="postgres", password="rooter") as conn:
        conn: psycopg2.extensions.connection
        with conn.cursor() as cur:
            cur: psycopg2.extensions.cursor
            cur.execute(SELECT_DIRECTS_USER, vars=(user,))

            my_dirs = cur.fetchall()

            data = defaultdict(dict)

            for university, direct, rating, category_id, ctrl_number in my_dirs:
                cur.execute(SELECT_COUNT_USERS_CONSENT_ON_OTHER_DIRECTS,
                            vars={'category': category_id, 'rating': rating})
                consent_on_other = cur.fetchone()[0]

                cur.execute(SELECT_COUNT_USERS_CONSENT_ON_DIRECT, vars={'category': category_id, 'rating': rating})
                consent = cur.fetchone()[0]
                data[university][direct] = {
                    'real_rating': rating - consent_on_other - consent,
                    'consent': consent,
                    'ctrl_number': ctrl_number
                }
    return jsonify(data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
