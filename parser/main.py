import asyncio
import difflib
import enum
import logging
import os
import socket
import sys
import time
from typing import Set, Optional

import aiohttp
import psycopg2.extensions
import requests
from lxml import html
from selenium import webdriver
from selenium.webdriver import ChromeOptions, ChromeService
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

YEAR = os.getenv("PARSE_YEAR")

URL_PSU = f"http://www.psu.ru/files/docs/priem-{YEAR}/"
URL_PSTU = f"https://pstu.ru/enrollee/stat{YEAR}/pol{YEAR}/"
TMP_PSTU = "pstu"


class University:
    ALL: list["University"] = []

    def __init__(self, name: str, city: str):
        self.name = name
        self.city = city

        CUR.execute(
            f"insert into universities (name, city) "
            f"values ({repr(name)}, {repr(city)}) "
            f"on conflict (name) do nothing"
        )

        self._faculties: list[Faculty] = []
        University.ALL.append(self)

    def add_faculty(self, faculty: "Faculty"):
        self._faculties.append(faculty)


class Faculty:
    _count = 0

    def __init__(self, university: "University", name: str):
        self.id = Faculty._count
        Faculty._count += 1

        self.uni = university
        self.name = name

        CUR.execute(
            f"insert into faculties(id, university, name) "
            f"values ({self.id}, {repr(self.uni.name)}, {repr(self.name)}) "
            f"on conflict (id) do nothing"
        )

        self.uni.add_faculty(self)
        self._directs = []

    def add_direct(self, direct: "Direct"):
        self._directs.append(direct)

    @classmethod
    def from_name_and_uni(cls, university: "University", name: str):
        for faculty in university._faculties:
            if faculty.name == name:
                return faculty
        return cls(university, name)


class Direct:
    _count = 0

    class Group:
        _count = 0

        class Type(enum.Enum):
            BUDGET = "Бюджет"
            CONTRACT = "Договор"
            TARGET = "Целевое"

        NAME2ID_TYPES = {
            Type.BUDGET: 0,
            Type.CONTRACT: 1,
            Type.TARGET: 2
        }

        def __init__(self, direct: "Direct", type_group: "Direct.Group.Type"):
            self.id = Direct.Group._count
            Direct.Group._count += 1

            self.direct = direct
            self.type = type_group

            CUR.execute(
                f"insert into groups(id, direct, type) "
                f"values ({self.id}, {self.direct.id}, {Direct.Group.NAME2ID_TYPES[self.type]}) "
                f"on conflict (id) do nothing"
            )

            self._categories: Set["Direct.Category"] = set()

        def __getitem__(self, category_type: "Direct.Category.Type") -> "Direct.Category":
            for category in self._categories:
                if category.type == category_type:
                    return category
            category = Direct.Category(self, category_type)
            self._categories.add(category)
            return category

        def __hash__(self):
            return hash(self.type)

    class Category:
        _count = 0

        class Type(enum.Enum):
            MAIN = "Общий конкурс"
            SPECIAL = "Особое право"
            EXTRA = "Специальная квота"
            WEE = "БВИ"
            FOREIGN = "Иностранцы"
            TARGET = "Целевое"

        NAME2ID_TYPES = {
            Type.MAIN: 0,
            Type.SPECIAL: 1,
            Type.EXTRA: 2,
            Type.WEE: 3,
            Type.FOREIGN: 4,
            Type.TARGET: 5
        }

        class CtrlNumber:
            def __init__(self, total: int, has: int):
                self.total = total
                self.has = has
                assert total >= has

        def __init__(self, group: "Direct.Group", type_category: "Direct.Category.Type"):
            self.id = Direct.Category._count
            Direct.Category._count += 1

            self.group = group
            self.type = type_category
            self._ctrl_number: int = 0
            CUR.execute(
                f"""insert into categories(id, "group", type, ctrl_number) """
                f"values ("
                f"{self.id}, {self.group.id}, {Direct.Category.NAME2ID_TYPES[self.type]}, {self._ctrl_number}"
                f") "
                f"on conflict (id) do nothing"
            )

        @property
        def ctrl_number(self):
            return self._ctrl_number

        @ctrl_number.setter
        def ctrl_number(self, value: int):
            assert isinstance(value, int), "Error!"
            self._ctrl_number = value
            CUR.execute(f"UPDATE categories SET ctrl_number = {self._ctrl_number} WHERE id = {self.id}")

        def __hash__(self):
            return hash(self.type)

    def __init__(self, faculty: "Faculty", name: str, form: str, level: str):
        self.id = Direct._count
        Direct._count += 1

        self.faculty = faculty

        self.name = name
        self.form = form
        self.level = level
        self._groups: Set["Direct.Group"] = set()

        CUR.execute(
            f"insert into directs(id, name, form, level, faculty) "
            f"values ({self.id}, {repr(self.name)}, {repr(self.form)}, {repr(self.level)}, {self.faculty.id}) "
            f"on conflict (id) do nothing"
        )

        self.faculty.add_direct(self)

    def __getitem__(self, group_type: "Direct.Group.Type") -> "Direct.Group":
        for group in self._groups:
            if group.type == group_type:
                return group
        group = Direct.Group(self, group_type)
        self._groups.add(group)
        return group

    def __hash__(self):
        return hash(str(self))

    @classmethod
    def from_name_and_fac(cls, faculty: "Faculty", name: str, form: str, level: str):
        for direct in faculty._directs:
            if direct.name == name and direct.form == form and direct.level == level:
                return direct
        return cls(faculty, name, form, level)


class User:
    ALL: Set["User"] = set()

    def __init__(self, snils: str):
        self.snils = snils.replace("-", "").replace(" ", "")
        self.name = None

        CUR.execute(
            f"""insert into users(id)"""
            f"values ({repr(self.snils)})"
            f"on conflict (id) do nothing "
        )

        self.requests: Set["Request"] = set()
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
    def add(rating: int, user: "User", category: "Direct.Category", original_doc: bool, total_sum: int = 0):
        CUR.execute(
            f"""insert into requests(id, rating, "user", category, total_sum, original_doc)"""
            f"values ("
            f"{Request._count}, {rating}, {repr(user.snils)}, {category.id}, {total_sum}, {original_doc}"
            f")"
        )
        Request._count += 1

    @staticmethod
    def reset():
        CUR.execute("delete from requests")
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
    psu = University('ПГНИУ', 'Пермь')
    logging.info(f"Parsing PSU")

    logging.info(f"Download tables")
    psu_data = requests.get(URL_PSU).content.decode('utf8')
    directs_data = html.fromstring(psu_data).xpath("//article")

    logging.info("Parse")
    for direct_data in directs_data:
        # Парс
        level_form, fac_name, dir_name = list(map(lambda x: x.text, direct_data.find("h2").findall("span")))
        level, form = map(lambda x: x.strip("(").strip(")"), level_form.split(" "))
        group_names = list(map(lambda x: x.text, direct_data.findall("h3")))
        tables = list(map(lambda x: x.findall("tr"), direct_data.findall("table")))
        numbers_data = list(map(lambda x: list(map(
            lambda y: int(y.text or 0), x)), direct_data.findall("p")[2:]))

        sub_numbers_data = list(map(
            lambda x: list(map(
                lambda y: (y.text.strip()[:-1], Direct.Category.CtrlNumber(
                    *list(map(lambda z: int(z.text), y.findall("strong"))))),
                x.findall("li"))),
            direct_data.findall("ul")))

        # Добавление факультета и направления
        faculty = Faculty.from_name_and_uni(psu, fac_name)
        direct = Direct.from_name_and_fac(faculty, dir_name, form, level)

        # Добавление недостающих списков для таблиц
        while len(group_names) > len(tables):
            tables.append([])

        # Добавление списков и заявлений
        for group_name, table in zip(group_names, tables):

            if group_name == "Бюджетные места":
                main_ctrl_number = Direct.Category.CtrlNumber(*numbers_data.pop(0))
                group = direct[Direct.Group.Type.BUDGET]
                group[Direct.Category.Type.MAIN].ctrl_number = main_ctrl_number.has
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
            elif group_name == "По договорам":
                numbers = numbers_data.pop(0)
                main_ctrl_number = Direct.Category.CtrlNumber(*numbers[:2])
                group = direct[Direct.Group.Type.CONTRACT]
                group[Direct.Category.Type.MAIN].ctrl_number = main_ctrl_number.has
                if len(numbers) == 5:
                    foreign_ctrl_number = Direct.Category.CtrlNumber(*numbers[3:])
                    main_ctrl_number.remove(foreign_ctrl_number)
                    direct[Direct.Group.Type.CONTRACT][
                        Direct.Category.Type.FOREIGN].ctrl_number = foreign_ctrl_number.has
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


def parse_pstu():
    pstu = University("ПНИПУ", "Пермь")
    logging.info(f"Parse PSTU")

    logging.info("Download links")
    options = ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    with webdriver.Firefox(service=ChromeService(ChromeDriverManager().install()), options=options) as driver:
        driver.get(URL_PSTU)
        main_elem = driver.find_element(By.CLASS_NAME, value="pol2013")
        links = list(map(lambda x: x.get_attribute("href"), main_elem.find_elements(By.XPATH, value=".//ul/li/a")))

    logging.info("Download tables")
    async_example = AsnycGrab(links, 100)
    async_example.eventloop()

    filenames = os.listdir("pstu")
    group_aliases = {
        "Бюджетная основа": Direct.Group.Type.BUDGET,
        "Полное возмещение затрат": Direct.Group.Type.CONTRACT
    }
    category_aliases = {
        "На общих основаниях": Direct.Category.Type.MAIN,
        "Имеющие особое право": Direct.Category.Type.SPECIAL
    }

    logging.info("Parse")
    for i, filename in enumerate(filenames):
        if not filename.endswith(".html"):
            continue

        with open(f"{TMP_PSTU}/{filename}", encoding="utf8") as file:
            pstu_data = file.read()

        direct_data = html.fromstring(pstu_data).xpath("//tr")
        form = direct_data[3].find("td").text.replace("Форма обучения - ", "")
        faculty_name = direct_data[4].find("td").text.replace("Подразделение - ", "")
        level = direct_data[5].find("td").text.replace("Уровень подготовки - ", "")
        _ = direct_data[6].find("td").text.replace("Направление подготовки/специальность - ", "")  # Unused
        group_name = direct_data[7].find("td").text.replace("Основание поступления - ", "")
        category_name = direct_data[8].find("td").text.replace("Категория приема - ", "")
        real_direct_name = direct_data[9].find("td").text.replace("Конкурсная группа - ", "")
        ctrl_number_inner = direct_data[10].find("td").text
        titles = list(map(lambda x: x.text, direct_data[12].findall("td")))
        reqs = direct_data[13:]

        faculty = Faculty.from_name_and_uni(pstu, faculty_name)
        direct = Direct.from_name_and_fac(faculty, real_direct_name, form, level)
        ctrl_number = Direct.Category.CtrlNumber(int(ctrl_number_inner.split()[2][:-1]),
                                                 int(ctrl_number_inner.split()[-1][:-1]))
        if group_name == "Целевой приём" or group_name == "Целевой прием":
            group = direct[Direct.Group.Type.BUDGET]
            category = group[Direct.Category.Type.TARGET]
        else:
            group = direct[group_aliases[group_name]]
            category = group[category_aliases[category_name]]
        category.ctrl_number = ctrl_number.total

        index_data = {
            "rating": {
                "i": None,
                "aliases": [
                    "№",
                    "Номер ПП"
                ]
            }, "snils": {
                "i": None,
                "aliases": [
                    "Уникальный код"
                ]
            }, "total_sum": {
                "i": None,
                "aliases": [
                    "Сумма баллов"
                ]
            }, "original_doc": {
                "i": None,
                "aliases": [
                    "Оригинал в вузе",
                    "Вид документа",
                    "Вид документа об образовании"
                ]
            }
        }

        index_title = accept_indexes(index_data, titles)

        for req_data in reqs:
            cols = req_data.findall("td")
            rating = int(cols[index_title["rating"]].text)
            snils = cols[index_title["snils"]].text
            total_sum = int(cols[index_title["total_sum"]].text) if index_title["total_sum"] is not None else 0
            original_doc = difflib.SequenceMatcher(
                a="Оригинал",
                b=(cols[index_title["original_doc"]].text or "").strip()
            ).ratio() > 0.9

            Request.add(rating, User.from_snils(snils), category,
                        total_sum=total_sum, original_doc=original_doc)

    logging.info(f"PSTU ready!")


def parse_all():
    logging.info("Parsing started...")
    t = time.time()
    Request.reset()
    CONN.commit()
    parse_psu()
    parse_pstu()
    CUR.execute(f"UPDATE update_data SET last_update_timestamp = {t}")
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
