import difflib
import logging
import socket
import re
import psycopg2.extensions
import requests
import sys
import time
from lxml import html

YEAR = 2022  # os.getenv("PARSE_YEAR")


class University:
    ALL: list["University"] = []

    def __init__(self, *, name: str, city: str):
        self.name = name
        self.city = city

        self._directs: list["Direct"] = []
        University.ALL.append(self)

    @property
    def directs(self) -> list["Direct"]:
        return self._directs

    @staticmethod
    def reset(cur):
        for uni in University.ALL:
            uni._directs.clear()
        cur.execute("truncate table universities CASCADE")


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

    def __init__(self, *, university: "University", faculty: str, code: str, name: str, form: str, level: str):
        Direct._count += 1

        self.id = Direct._count

        self.uni = university
        self.uni._directs.append(self)

        self.faculty = faculty
        self.code = code
        self.name = name
        self.form = form
        self.level = level

        self._groups: set["Group"] = set()

    def __getitem__(self, key: tuple["Group.GroupType", "Group.CategoryType"]) -> "Group":
        group_type, category_type = key

        for group in self.groups:
            if group.group_type != group_type:
                continue
            if group.category_type != category_type:
                continue
            return group

        group = Group(direct=self, group_type=group_type, category_type=category_type, ctrl_number=1)
        self._groups.add(group)
        return group

    def __hash__(self):
        return hash(self.id)

    @property
    def groups(self) -> set["Group"]:
        return self._groups

    @staticmethod
    def reset(cur):
        cur.execute("truncate table directs CASCADE")
        Direct._count = 0


class Group:
    _count = 0

    class GroupType:
        BUDGET = "Бюджет"
        CONTRACT = "Договор"
        TARGET = "Целевое"

    class CategoryType:
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
        self.ctrl_number = ctrl_number

        self._requests: list["Request"] = list()

    def __hash__(self):
        return hash(f"{self.id}")

    @property
    def requests(self) -> list["Request"]:
        return self._requests

    @staticmethod
    def reset(cur):
        cur.execute("truncate table groups CASCADE")
        Group._count = 0


class User:
    ALL: set["User"] = set()

    def __init__(self, snils: str):
        self.snils = snils.replace("-", "").replace(" ", "")
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

    @staticmethod
    def reset(cur):
        cur.execute("truncate table users CASCADE")
        User.ALL.clear()


class Request:
    _count = 0

    def __init__(self, group: "Group", user: "User", rating: int, total_sum: int, original_doc: bool,
                 priority: int = 1):
        self.group = group
        self.user = user
        self.rating = rating
        self.total_sum = total_sum
        self.original_doc = original_doc
        self.priority = priority

        self.group._requests.append(self)

        Request._count += 1

    def __hash__(self):
        return hash("Request:" + self.user.snils)

    @staticmethod
    def reset(cur):
        cur.execute("truncate table requests CASCADE")
        Request._count = 0


class Parser:
    def __init__(self, url: str, name_uni: str, city_uni: str):
        self.url = url
        self.uni = University(name=name_uni, city=city_uni)

    def parse_applicant_list(self, url: str):
        pass

    def save_to_db(self, cur):
        cur.execute(
            f"insert into universities(name, city) "
            f"values ('{self.uni.name}', '{self.uni.city}')"
        )

        for direct in self.uni.directs:
            cur.execute(
                f"insert into directs(university, faculty, code, name, form, level, id) "
                f"values ('{self.uni.name}', '{direct.faculty}', '{direct.code}', "
                f"'{direct.name}', %(direct_form)s, %(direct_level)s, {direct.id})",
                vars={
                    "direct_form": direct.form,
                    "direct_level": direct.level
                }
            )

            for group in direct.groups:
                cur.execute(
                    f"insert into groups(id, direct, group_type, category_type, ctrl_number) "
                    f"values ({group.id}, {direct.id}, %(group_group_type)s, "
                    f"%(group_category_type)s, {group.ctrl_number}) ",
                    vars={
                        "group_group_type": group.group_type,
                        "group_category_type": group.category_type
                    }
                )
                for request in group.requests:
                    cur.execute(
                        f"insert into users(snils) "
                        f"values ('{request.user.snils}')"
                        f"on conflict (snils) do nothing"
                    )
                    cur.execute(
                        f"insert into requests(group_id, \"user\", rating, total_sum, original_doc, year, priority)"
                        f"values ({group.id}, '{request.user.snils}', {request.rating}, {request.total_sum}, "
                        f"{request.original_doc}, {YEAR}, {request.priority})"
                        f"on conflict (group_id, \"user\") do nothing"
                    )

    @staticmethod
    def reset_applicants_data(cur):
        Request.reset(cur)
        Group.reset(cur)
        Direct.reset(cur)
        University.reset(cur)

    @staticmethod
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

    @staticmethod
    def wain_conn():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while True:
            try:
                s.connect(('db', 5432))
                s.close()
                break
            except socket.error:
                time.sleep(0.1)


class PSUParser(Parser):
    URL = f"http://www.psu.ru/files/docs/priem-2022"

    def __init__(self):
        super().__init__(PSUParser.URL, 'ПГНИУ', "Пермь")

    def parse_applicant_list(self, url: str):
        logging.info(f"Parsing PSU")

        logging.info(f"Download tables")
        psu_data = requests.get(url).content.decode('utf8')
        directs_data = html.fromstring(psu_data).xpath("//article")

        logging.info("Parse")
        for direct_data in directs_data:
            # Парс
            level_form, faculty, dir_name_code = list(map(lambda x: x.text, direct_data.find("h2").findall("span")))
            dir_code = re.search(r'\(\d?\d\.\d?\d\.\d?\d\)', dir_name_code).group()
            dir_name = dir_name_code.replace(" " + dir_code, "").replace("  ", " ")
            dir_code = ".".join(map(lambda x: x.rjust(2, "0"), dir_code[1:-1].split(".")))

            level, form = map(lambda x: x.strip("(").strip(")"), level_form.split(" "))
            form = (
                Direct.Form.IN_PA if form == "очно-заочная"
                else (
                    Direct.Form.IN_P if form == "очная"
                    else Direct.Form.IN_A
                )
            )

            level = (
                Direct.Level.BACHELOR if "бакал" in level.lower()
                else (
                    Direct.Level.GRADUATE if "аспер" in level.lower()
                    else (
                        Direct.Level.MASTER if "магист" in level.lower()
                        else Direct.Level.BACHELOR  # TODO Добавить поддержку Direct.Level.SPECIALITY
                    )
                )
            )

            group_names = list(map(lambda x: x.text, direct_data.findall("h3")))
            tables = list(map(lambda x: x.findall("tr"), direct_data.findall("table")))
            numbers_data = list(map(lambda x: list(map(
                lambda y: int(y.text or 0), x)), direct_data.findall("p")[2:]))
            sub_numbers_data = list(map(
                lambda x: list(map(
                    lambda y: (y.text.strip()[:-1], int(list(map(lambda z: int(z.text), y.findall("strong")))[0])),
                    x.findall("li"))),
                direct_data.findall("ul")))

            direct = Direct(university=self.uni, faculty=faculty, code=dir_code, name=dir_name, form=form,
                            level=level)

            # Добавление недостающих списков для таблиц
            while len(group_names) > len(tables):
                tables.append([])

            # Добавление списков и заявлений
            for group_name, table in zip(group_names, tables):
                if group_name == "Бюджетные места":
                    main_ctrl_number = numbers_data.pop(0)[0]

                    group_type = Group.GroupType.BUDGET
                    if numbers_data and not numbers_data[0]:
                        numbers_data.pop(0)
                        for category_name, ctrl_number in sub_numbers_data.pop(0):
                            if category_name == 'Квота приёма лиц, имеющих особые права':
                                direct[(Group.GroupType.BUDGET, Group.CategoryType.SPECIAL)].ctrl_number = ctrl_number
                            elif category_name == 'Квота приёма на целевое обучение':
                                direct[(Group.GroupType.BUDGET, Group.CategoryType.TARGET)].ctrl_number = ctrl_number
                            elif category_name == 'Специальная квота':
                                direct[(Group.GroupType.BUDGET, Group.CategoryType.EXTRA)].ctrl_number = ctrl_number
                            else:
                                assert False, f"Неизвестная квота {repr(category_name)}"
                            main_ctrl_number -= ctrl_number
                    direct[(group_type, Group.CategoryType.MAIN)].ctrl_number = main_ctrl_number

                elif group_name == "По договорам":
                    numbers = numbers_data.pop(0)
                    main_ctrl_number = numbers[:2][0]
                    group_type = Group.GroupType.CONTRACT
                    if len(numbers) >= 5:
                        foreign_ctrl_number = numbers[3:][0]
                        main_ctrl_number -= foreign_ctrl_number
                        direct[(group_type, Group.CategoryType.FOREIGN)].ctrl_number = foreign_ctrl_number
                    direct[(group_type, Group.CategoryType.MAIN)].ctrl_number = main_ctrl_number
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
                index_title = self.accept_indexes(index_data, titles)

                group = None
                for i, row in enumerate(reqs):
                    cols = row.findall("td")
                    if len(cols) == 1:
                        category_name = row[0].find("strong").text
                        if category_name == "Общий конкурс":
                            group = direct[(group_type, Group.CategoryType.MAIN)]
                        elif category_name == "Без экзаменов":
                            group = direct[(group_type, Group.CategoryType.WEE)]
                        elif category_name == "Особая квота":
                            group = direct[(group_type, Group.CategoryType.SPECIAL)]
                        elif category_name == "Специальная квота":
                            group = direct[(group_type, Group.CategoryType.EXTRA)]
                        elif "Целевая квота" in category_name:
                            group = direct[(group_type, Group.CategoryType.TARGET)]
                        else:
                            assert False, f"Неизвестная категория {repr(category_name)}"
                    else:
                        rating = int(cols[index_title["rating"]].text or 0)
                        snils = cols[index_title["snils"]].find("font").text or ""
                        original_doc = bool((cols[index_title["original_doc"]].text or '').strip())
                        total_sum = int(cols[index_title["total_sum"]].text or 0)

                        Request(group, User.from_snils(snils), rating, total_sum, original_doc)

        logging.info(f"PSU ready!")


def main():
    parsers = [PSUParser()]
    Parser.wain_conn()
    while True:
        try:
            with psycopg2.connect(host="db", database="postgres", user="postgres", password="rooter") as con:
                with con.cursor() as cur:
                    con: psycopg2.extensions.connection
                    cur: psycopg2.extensions.cursor

                    cur.execute("SELECT last_update_timestamp FROM update_data")
                    update_time = 12 * 60 * 60
                    w = max(0, int(update_time - (time.time() - cur.fetchone()[0])))
                    logging.info(f"Started! Next update after {w}s")
                    time.sleep(w)

                    while True:
                        logging.info("Parsing started...")
                        t = time.time()
                        Parser.reset_applicants_data(cur)
                        con.commit()

                        for parser in parsers:
                            parser.parse_applicant_list(parser.url)
                            parser.save_to_db(cur)
                        con.commit()
                        logging.info(f"Parsing completed at {time.time() - t}s")

                        time.sleep(update_time)
        except psycopg2.OperationalError:
            time.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO, stream=sys.stdout)
    main()
    # p = PSUParser()
    # p.parse_applicant_list(PSUParser.URL)
    # p.save_to_db(None)
