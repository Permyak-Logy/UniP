import json
import logging
import sys
from collections import defaultdict
import applicant_service
import psycopg2.extensions
from flask import Flask, Response

from queries import *

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False


@app.route('/get_real_rating/<user>')
def get_real_rating(user: str):
    with psycopg2.connect(host="db", database="postgres", user="postgres", password="rooter") as conn:
        conn: psycopg2.extensions.connection
        with conn.cursor() as cur:
            cur: psycopg2.extensions.cursor
            data = applicant_service.get_real_rating(user, cur)
    return Response(json.dumps(data, ensure_ascii=False), content_type="application/json; charset=utf-8")


@app.route('/get_my_directs/<user>')
def get_my_directs(user: str):
    pass


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO, stream=sys.stdout)
    app.run(host="0.0.0.0", port=80)
