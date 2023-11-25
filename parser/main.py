import asyncio
import difflib
import os
from time import time

import aiohttp
import requests
from lxml import html
from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver import FirefoxOptions, FirefoxService
from models import *

PSU = University("Пермь", "ПГНИУ")
PSTU = University("Пермь", "Политех")

TMP_PSU = "psu"
TMP_PSTU = "pstu"


def uprint(*args):
    print("\r" + " " * 1000 + "\r" + " ".join(map(str, args)), end='')


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


def parse_psu(load=False):
    if load:
        uprint(f"{PSU} Загрузка данных...")
        with open("psu_data.html", mode='wb') as file:
            file.write(requests.get("http://www.psu.ru/files/docs/priem-2023/").content)
    with open("psu_data.html", encoding="utf8") as file:
        uprint(f"{PSU} Чтение...")
        psu_data = file.read()

    directs_data = html.fromstring(psu_data).xpath("//article")
    count_directs = len(directs_data)
    uprint(f"{PSU} Парсинг 0.0%")

    for i, direct_data in enumerate(directs_data):
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
        faculty = Faculty.from_name_and_uni(PSU, fac_name)
        direct = Direct.from_name_and_fac(faculty, dir_name, form, level)

        # Добавление недостающих списков для таблиц
        while len(group_names) > len(tables):
            tables.append([])

        # Добавление списков и заявлений
        for group_name, table in zip(group_names, tables):

            if group_name == "Бюджетные места":
                main_ctrl_number = Direct.Category.CtrlNumber(*numbers_data.pop(0))
                group = direct[Direct.Group.Type.BUDGET]
                group[Direct.Category.Type.MAIN].ctrl_number = main_ctrl_number
                if numbers_data and not numbers_data[0]:
                    numbers_data.pop(0)
                    for category_name, ctrl_number in sub_numbers_data.pop(0):
                        if category_name == 'Квота приёма лиц, имеющих особые права':
                            direct[Direct.Group.Type.BUDGET][Direct.Category.Type.SPECIAL].ctrl_number = ctrl_number
                        elif category_name == 'Квота приёма на целевое обучение':
                            direct[Direct.Group.Type.BUDGET][Direct.Category.Type.TARGET].ctrl_number = ctrl_number
                        elif category_name == 'Специальная квота':
                            direct[Direct.Group.Type.BUDGET][Direct.Category.Type.EXTRA].ctrl_number = ctrl_number
                        else:
                            assert False, f"Неизвестная квота {repr(category_name)}"
                        main_ctrl_number.remove(ctrl_number)
            elif group_name == "По договорам":
                numbers = numbers_data.pop(0)
                main_ctrl_number = Direct.Category.CtrlNumber(*numbers[:2])
                group = direct[Direct.Group.Type.CONTRACT]
                group[Direct.Category.Type.MAIN].ctrl_number = main_ctrl_number
                if len(numbers) == 5:
                    foreign_ctrl_number = Direct.Category.CtrlNumber(*numbers[3:])
                    main_ctrl_number.remove(foreign_ctrl_number)
                    direct[Direct.Group.Type.CONTRACT][Direct.Category.Type.FOREIGN].ctrl_number = foreign_ctrl_number
            else:
                assert False, f"Неизвестная группа {repr(group_name)}"

            if not table:
                continue
            titles_inner, category_name_inner, *reqs = table
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

            category_name = category_name_inner.find("td").find("strong").text

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

            for row in reqs:
                cols = row.findall("td")

                rating = int(cols[index_title["rating"]].text or 0)
                snils = cols[index_title["snils"]].find("font").text or ""
                original_doc = bool(cols[index_title["original_doc"]].text)
                total_sum = int(cols[index_title["total_sum"]].text or 0)

                req = Request(rating, Student.from_snils(snils), category,
                              total_sum=total_sum, original_doc=original_doc)
                category.requests.append(req)

        uprint(f"{PSU} Парсинг {round((i + 1) / count_directs * 100, 2)}%")

    uprint(f"{PSU} Готово!")
    print()


def parse_pstu(load=False, load_links=False):
    if load_links:
        options = FirefoxOptions()
        options.add_argument("--headless")
        with webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()), options=options) as driver:
            driver.get("https://pstu.ru/enrollee/stat2023/pol2023/")
            main_elem = driver.find_element(By.CLASS_NAME, value="pol2013")
            uprint(f"{PSTU} Загрузка ссылок...")
            links = list(map(lambda x: x.get_attribute("href"), main_elem.find_elements(By.XPATH, value=".//ul/li/a")))

        uprint(f"{PSTU} Скачивание 0%")
        with open(f"{TMP_PSTU}/pstu_data.txt", mode='w') as file:
            file.write("\n".join(links))

    if load:
        class AsnycGrab(object):

            def __init__(self, url_list, max_threads):
                self.urls = url_list
                self.downloaded = set()
                self.max_threads = max_threads  # Номер процесса

            @staticmethod
            async def download(url, filename):
                # Отправлять запрос асинхронно
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=30) as response:
                        assert response.status == 200, f"Response status: {response.status}"
                        with open(filename, mode='wb') as file:
                            file.write(await response.read())

            async def handle_tasks(self, task_id, work_queue):
                while not work_queue.empty():
                    index, current_url = await work_queue.get()  # Получить url из очереди
                    try:
                        await self.download(current_url.strip(), f"{TMP_PSTU}/pstu_data{str(index).rjust(4, '0')}.html")
                        self.downloaded.add(current_url)
                    except Exception as e:
                        print(e, current_url)

            async def handle_progress(self, task_id):
                uprint(f"{PSTU} Скачивание 0.0%")
                while True:
                    uprint(f"{PSTU} Скачивание {round(len(self.downloaded) / len(self.urls) * 100, 2)}%")
                    if len(self.downloaded) == len(self.urls):
                        break
                    await asyncio.sleep(0.01)

            def eventloop(self):
                if not os.path.isdir(TMP_PSTU):
                    os.mkdir(TMP_PSTU)
                q = asyncio.Queue()  # Coroutine queue
                [q.put_nowait((i, url)) for i, url in enumerate(self.urls)]  # Обсуждение url все поставлено в очередь
                loop = asyncio.get_event_loop()  # Создать цикл событий

                tasks = [self.handle_tasks(task_id, q, ) for task_id in range(self.max_threads)]
                tasks.append(self.handle_progress(len(self.urls) + 1))
                loop.run_until_complete(asyncio.wait(tasks))

        with open(f"{TMP_PSTU}/pstu_data.txt") as file:
            links = file.readlines()
        async_example = AsnycGrab(links, 100)
        async_example.eventloop()
    filenames = os.listdir("pstu")
    uprint(f"Парсинг 0.0%")
    group_aliases = {
        "Бюджетная основа": Direct.Group.Type.BUDGET,
        "Полное возмещение затрат": Direct.Group.Type.CONTRACT
    }
    category_aliases = {
        "На общих основаниях": Direct.Category.Type.MAIN,
        "Имеющие особое право": Direct.Category.Type.SPECIAL
    }

    for i, filename in enumerate(filenames):
        if not filename.endswith(".html"):
            continue

        with open(f"{TMP_PSTU}/{filename}", encoding="utf8") as file:
            pstu_data = file.read()

        direct_data = html.fromstring(pstu_data).xpath("//tr")
        form = direct_data[3].find("td").text.replace("Форма обучения - ", "")
        faculty_name = direct_data[4].find("td").text.replace("Подразделение - ", "")
        level = direct_data[5].find("td").text.replace("Уровень подготовки - ", "")
        direct_name = direct_data[6].find("td").text.replace("Направление подготовки/специальность - ", "")  # Unused
        group_name = direct_data[7].find("td").text.replace("Основание поступления - ", "")
        category_name = direct_data[8].find("td").text.replace("Категория приема - ", "")
        real_direct_name = direct_data[9].find("td").text.replace("Конкурсная группа - ", "")
        ctrl_number_inner = direct_data[10].find("td").text
        titles = list(map(lambda x: x.text, direct_data[12].findall("td")))
        reqs = direct_data[13:]

        faculty = Faculty.from_name_and_uni(PSTU, faculty_name)
        direct = Direct.from_name_and_fac(faculty, real_direct_name, form, level)
        ctrl_number = Direct.Category.CtrlNumber(int(ctrl_number_inner.split()[2][:-1]),
                                                 int(ctrl_number_inner.split()[-1][:-1]))
        if group_name == "Целевой приём" or group_name == "Целевой прием":
            group = direct[Direct.Group.Type.BUDGET]
            category = group[Direct.Category.Type.TARGET]
        else:
            group = direct[group_aliases[group_name]]
            category = group[category_aliases[category_name]]
        category.ctrl_number = ctrl_number

        index_data = {
            "rating": {
                "i": None,
                "aliases": [
                    "№"
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
                    "Вид документа"
                ]
            }
        }

        index_title = accept_indexes(index_data, titles)

        for req_data in reqs:
            cols = req_data.findall("td")
            rating = int(cols[index_title["rating"]].text) if index_title["rating"] is not None else 0
            snils = cols[index_title["snils"]].text
            total_sum = int(cols[index_title["total_sum"]].text) if index_title["total_sum"] is not None else 0
            original_doc = difflib.SequenceMatcher(
                a="Оригинал",
                b=(cols[index_title["original_doc"]].text or "").strip()
            ).ratio() > 0.9
            req = Request(rating, Student.from_snils(snils), category, total_sum=total_sum, original_doc=original_doc)
            category.requests.append(req)

        uprint(f"{PSTU} Парсинг {round((i + 1) / len(filenames) * 100, 2)}%")
    uprint(f"{PSTU} Готово!")
    print()


def parse_all(load=False):
    t = time()
    parse_psu(load)
    parse_pstu(load, load_links=load)
    print("Парсинг завершён за", time() - t, "секунд")


if __name__ == "__main__":
    parse_pstu(load=False, load_links=False)
