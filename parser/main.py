import asyncio
import difflib
import enum
import logging
import os
import socket
import sys
import time

import aiohttp
import psycopg2.extensions
import requests
from lxml import html
from selenium import webdriver
from selenium.webdriver import ChromeOptions, ChromeService
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

YEAR = 2024  # os.getenv("PARSE_YEAR")

URL_PSU = f"http://www.psu.ru/files/docs/priem-{YEAR}/"
URL_PSTU = f"https://pstu.ru/enrollee/stat{YEAR}/pol{YEAR}/"
TMP_PSTU = "pstu"


class Direct:
    _count = 0

    class Form:
        IN_P = "Очно"
        IN_A = "Заочно"
        IN_PA = "Очно/Заочно"

    class Level:
        BACHELOR = "Бакалавриат"
        MASTER = "Магистратура"
        GRADUATE = "Асперантура"
        SPECIALITY = "Специалитет"

    def __init__(self, *, university: str, faculty: str, code: str, name: str, form: str, level: str):
        Direct._count += 1

        self.id = Direct._count
        self.uni = university
        self.faculty = faculty
        self.code = code
        self.name = name
        self.form = form
        self.level = level

        self._groups: set["Group"] = set()

        CUR.execute(
            f"insert into directs(university, faculty, code, name, form, level, id) "
            f"values ('{self.uni}', '{self.faculty}', '{self.code}', "
            f"'{self.name}', '{self.form}', '{self.level}', {self.id})"
            f"on conflict (id) do nothing"
        )

    def __getitem__(self, key: tuple["Group.GroupType", "Group.CategoryType"]) -> "Group":
        group_type, category_type = key

        for group in self._groups:
            if group.group_type != group_type:
                continue
            if group.category_type != category_type:
                continue
            return group

        group = Group(direct=self, group_type=group_type, category_type=category_type, ctrl_number=1)
        self._groups.add(group)
        return group

    def __hash__(self):
        return hash(str(self))


class Group:
    _count = 0

    class GroupType(enum.Enum):
        BUDGET = "Бюджет"
        CONTRACT = "Договор"
        TARGET = "Целевое"

    class CategoryType(enum.Enum):
        MAIN = "Общий конкурс"
        SPECIAL = "Особое право"
        EXTRA = "Специальная квота"
        WEE = "БВИ"
        FOREIGN = "Иностранцы"
        TARGET = "Целевое"

    def __init__(self, *, direct: "Direct", group_type: "Group.GroupType", category_type: "Group.CategoryType",
                 ctrl_number: int):
        Group._count += 1

        self.id = Group._count
        self.direct = direct
        self.group_type = group_type
        self.category_type = category_type
        self._ctrl_number = ctrl_number

        CUR.execute(
            f"insert into groups(id, direct, group_type, category_type, ctrl_number) "
            f"values ({self.id}, {self.direct.id}, {self.group_type}, {self.category_type}, {self._ctrl_number}) "
            f"on conflict (id) do nothing"
        )

        self._requests: set["Request"] = set()

    @property
    def ctrl_number(self):
        return self._ctrl_number

    @ctrl_number.setter
    def ctrl_number(self, val):
        self._ctrl_number = val
        CUR.execute(f"update groups set ctrl_number={self._ctrl_number} where id={self.id}")

    def __hash__(self):
        return hash(f"{self.id}")


class User:
    ALL: set["User"] = set()

    def __init__(self, snils: str):
        self.snils = snils.replace("-", "").replace(" ", "")

        CUR.execute(
            f"insert into users(snils) "
            f"values ('{self.snils}')"
            f"on conflict (snils) do nothing "
        )

        self.requests: set["Request"] = set()
        User.ALL.add(self)

    def __hash__(self):
        return hash("Student:" + self.snils)

    @classmethod
    def from_snils(cls, snils: str):
        snils = snils.replace("-", "").replace(" ", "")
        for elem in User.ALL:
            if elem.snils == snils:
                return elem
        return cls(snils)


class Request:
    _count = 0

    @staticmethod
    def add(group: "Group", user: "User", rating: int, total_sum: int, original_doc: bool, priority: int = 1):
        CUR.execute(
            f"insert into requests(group_id, \"user\", rating, total_sum, original_doc, year, priority)"
            f"values ({group.id}, '{user.snils}', {rating}, {total_sum}, {original_doc}, {YEAR}, {priority})"
        )
        Request._count += 1

    @staticmethod
    def reset():
        CUR.execute("truncate table requests")
        Request._count = 0


class AsnycGrab(object):
    """Вспомогательный класс позволяющий быстро загрузить все таблицы ПНИПУ"""

    def __init__(self, url_list, max_threads):
        self.urls = url_list
        self.downloaded = set()
        self.max_threads = max_threads  # Номер процесса

    @staticmethod
    async def download(url, fn):
        # Отправлять запрос асинхронно
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=30) as response:
                assert response.status == 200, f"Response status: {response.status}"
                with open(fn, mode='wb') as f:
                    f.write(await response.read())

    async def handle_tasks(self, work_queue):
        while not work_queue.empty():
            index, current_url = await work_queue.get()  # Получить url из очереди
            try:
                await self.download(current_url.strip(), f"{TMP_PSTU}/pstu_data{str(index).rjust(4, '0')}.html")
                self.downloaded.add(current_url)
            except Exception as e:
                logging.warning(f"{e} {current_url}")

    async def handle_progress(self):
        while True:
            if len(self.downloaded) == len(self.urls):
                break
            await asyncio.sleep(0.01)

    def eventloop(self):
        if not os.path.isdir(TMP_PSTU):
            os.mkdir(TMP_PSTU)
        q = asyncio.Queue()  # Coroutine queue
        [q.put_nowait((i, url)) for i, url in enumerate(self.urls)]  # Обсуждение url все поставлено в очередь
        loop = asyncio.get_event_loop()  # Создать цикл событий

        tasks = [self.handle_tasks(q) for _ in range(self.max_threads)]
        tasks.append(self.handle_progress())
        loop.run_until_complete(asyncio.wait(tasks))


class Parser:
    def parse_applicant_list(self, url) -> list:
        pass

    def save_to_db(self):
        pass


class PSUParser(Parser):
    def parse_applicant_list(self, url):
        pass


def accept_indexes(index_data: dict, titles):
    to_find_title = set(index_data.keys())
    indexes = {key: None for key in index_data.keys()}

    for index, title in enumerate(titles):
        if not title:
            continue
        for check_title in to_find_title:
            if any(map(lambda x: difflib.SequenceMatcher(a=title, b=x).ratio() > 0.75,
                       index_data[check_title]["aliases"])):
                indexes[check_title] = index
                to_find_title.remove(check_title)
                break

    assert not (to_find_title - {"total_sum"}), f"Не найдены колонки {to_find_title} среди {titles}"
    return indexes


def wain_conn():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while True:
        try:
            s.connect(('db', 5432))
            s.close()
            break
        except socket.error:
            time.sleep(0.1)


def parse_psu():
    uni = 'ПГНИУ'

    logging.info(f"Parsing PSU")

    logging.info(f"Download tables")
    psu_data = requests.get(URL_PSU).content.decode('utf8')
    directs_data = html.fromstring(psu_data).xpath("//article")

    logging.info("Parse")
    for direct_data in directs_data:
        # Парс
        level_form, faculty, dir_name = list(map(lambda x: x.text, direct_data.find("h2").findall("span")))
        level, form = map(lambda x: x.strip("(").strip(")"), level_form.split(" "))
        group_names = list(map(lambda x: x.text, direct_data.findall("h3")))
        tables = list(map(lambda x: x.findall("tr"), direct_data.findall("table")))
        numbers_data = list(map(lambda x: list(map(
            lambda y: int(y.text or 0), x)), direct_data.findall("p")[2:]))

        sub_numbers_data = list(map(
            lambda x: list(map(
                lambda y: (y.text.strip()[:-1], Direct.CtrlNumber(
                    *list(map(lambda z: int(z.text), y.findall("strong"))))),
                x.findall("li"))),
            direct_data.findall("ul")))

        direct = Direct(university=uni, faculty=faculty, code="0.0.0", name=dir_name, form=form, level=level)

        # Добавление недостающих списков для таблиц
        while len(group_names) > len(tables):
            tables.append([])

        # Добавление списков и заявлений
        for group_name, table in zip(group_names, tables):

            if group_name == "Бюджетные места":
                main_ctrl_number = Direct.Category.CtrlNumber(*numbers_data.pop(0))
                group = direct[Direct.Group.Type.BUDGET]
                if numbers_data and not numbers_data[0]:
                    numbers_data.pop(0)
                    for category_name, ctrl_number in sub_numbers_data.pop(0):
                        if category_name == 'Квота приёма лиц, имеющих особые права':
                            direct[Direct.Group.Type.BUDGET][Direct.Category.Type.SPECIAL].ctrl_number = ctrl_number.has
                        elif category_name == 'Квота приёма на целевое обучение':
                            direct[Direct.Group.Type.BUDGET][Direct.Category.Type.TARGET].ctrl_number = ctrl_number.has
                        elif category_name == 'Специальная квота':
                            direct[Direct.Group.Type.BUDGET][Direct.Category.Type.EXTRA].ctrl_number = ctrl_number.has
                        else:
                            assert False, f"Неизвестная квота {repr(category_name)}"
                        main_ctrl_number.remove(ctrl_number)
                group[Direct.Category.Type.MAIN].ctrl_number = main_ctrl_number.has

            elif group_name == "По договорам":
                numbers = numbers_data.pop(0)
                main_ctrl_number = Direct.Category.CtrlNumber(*numbers[:2])
                group = direct[Direct.Group.Type.CONTRACT]
                if len(numbers) == 5:
                    foreign_ctrl_number = Direct.Category.CtrlNumber(*numbers[3:])
                    main_ctrl_number.remove(foreign_ctrl_number)
                    direct[Direct.Group.Type.CONTRACT][
                        Direct.Category.Type.FOREIGN].ctrl_number = foreign_ctrl_number.has

                group[Direct.Category.Type.MAIN].ctrl_number = main_ctrl_number.has
            else:
                assert False, f"Неизвестная группа {repr(group_name).encode().decode('utf8')} {faculty} {direct}"

            if not table:
                continue
            titles_inner, *reqs = table
            titles = list(map(lambda x: x.find("strong").text, titles_inner.findall("td")))
            index_data = {
                "rating": {
                    "i": None,
                    "aliases": [
                        "№ п/п"
                    ]
                }, "snils": {
                    "i": None,
                    "aliases": [
                        "СНИЛС или номер заявления"
                    ]
                }, "total_sum": {
                    "i": None,
                    "aliases": [
                        "Суммарный балл"
                    ]
                }, "original_doc": {
                    "i": None,
                    "aliases": [
                        "Оригинал документа"
                    ]
                }
            }
            index_title = accept_indexes(index_data, titles)
            category = None
            for i, row in enumerate(reqs):
                cols = row.findall("td")
                if len(cols) == 1:
                    category_name = row[0].find("strong").text
                    if category_name == "Общий конкурс":
                        category = group[Direct.Category.Type.MAIN]
                    elif category_name == "Без экзаменов":
                        category = group[Direct.Category.Type.WEE]
                    elif category_name == "Особая квота":
                        category = group[Direct.Category.Type.SPECIAL]
                    elif category_name == "Специальная квота":
                        category = group[Direct.Category.Type.EXTRA]
                    elif "Целевая квота" in category_name:
                        category = group[Direct.Category.Type.TARGET]
                    else:
                        assert False, f"Неизвестная категория {repr(category_name)}"
                else:
                    rating = int(cols[index_title["rating"]].text or 0)
                    snils = cols[index_title["snils"]].find("font").text or ""
                    original_doc = bool((cols[index_title["original_doc"]].text or '').strip())
                    total_sum = int(cols[index_title["total_sum"]].text or 0)

                    Request.add(rating, User.from_snils(snils), category,
                                total_sum=total_sum, original_doc=original_doc)

    logging.info(f"PSU ready!")


def parse_all():
    logging.info("Parsing started...")
    t = time.time()
    Request.reset()
    CONN.commit()
    parse_psu()

    CONN.commit()
    logging.info(f"Parsing completed at {time.time() - t}s")


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO, stream=sys.stdout)

    wain_conn()
    while True:
        try:
            with psycopg2.connect(host="db", database="postgres", user="postgres", password="rooter") as CONN:
                with CONN.cursor() as CUR:
                    CONN: psycopg2.extensions.connection
                    CUR: psycopg2.extensions.cursor

                    CUR.execute("SELECT last_update_timestamp FROM update_data")
                    update_time = 12 * 60 * 60
                    w = max(0, int(update_time - (time.time() - CUR.fetchone()[0])))
                    logging.info(f"Started! Next update after {w}s")
                    time.sleep(w)
                    while True:
                        parse_all()
                        time.sleep(update_time)
        except psycopg2.OperationalError:
            time.sleep(1)
