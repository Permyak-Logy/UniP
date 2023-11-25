import difflib
import enum
from typing import Set, Optional, Union
from const import *


class University:
    ALL: list["University"] = []

    def __init__(self, city: str, name: str):
        self.name = name
        self.city = city

        self._faculties: list[Faculty] = []
        University.ALL.append(self)

    def __str__(self):
        return f"{RC}U{BL_C}({NC}{self.name} г.{self.city}{BL_C}){NC}"

    def add_faculty(self, faculty: "Faculty"):
        self._faculties.append(faculty)

    def find_f(self, name) -> Optional["Faculty"]:
        pass

    def find_d(self, name) -> Optional["Direct"]:
        f = (None, 0)
        for faculty in self._faculties:
            for direct in faculty._directs:
                r = difflib.SequenceMatcher(None, direct.fullname(), name).ratio()
                f = max(f, (direct, r), key=lambda x: x[1])
        return f[0]


class Faculty:

    def __init__(self, university: "University", name: str):
        self.uni = university
        self.name = name

        self.uni.add_faculty(self)
        self._directs = []

    def __str__(self):
        return f"{RC}F{GR_C}({NC}{self.name}{GR_C}){NC}"

    def add_direct(self, direct: "Direct"):
        self._directs.append(direct)

    @classmethod
    def from_name_and_uni(cls, university: "University", name: str):
        for faculty in university._faculties:
            if faculty.name == name:
                return faculty
        return cls(university, name)

    def find_d(self, name) -> Optional["Direct"]:
        f = (None, 0)
        for direct in self._directs:
            r = difflib.SequenceMatcher(None, direct.name, name).ratio()
            if r >= 0.5:
                f = max(f, (direct, r), key=lambda x: x[1])
        return f[0]


class Direct:
    class Group:
        class Type(enum.Enum):
            BUDGET = "Бюджет"
            CONTRACT = "Договор"
            TARGET = "Целевое"

        def __init__(self, direct: "Direct", type_group: "Direct.Group.Type"):
            self.direct = direct
            self.type = type_group
            self._categories: Set["Direct.Category"] = set()

        def __str__(self):
            return f"{RC}G{NC}({self.type.name})"

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
        class Type(enum.Enum):
            MAIN = "Общий конкурс"
            SPECIAL = "Особое право"
            EXTRA = "Специальная квота"
            WEE = "БВИ"
            FOREIGN = "Иностранцы"
            TARGET = "Целевое"

        class CtrlNumber:
            def __init__(self, total: int, has: int):
                self.total = total
                self.has = has
                assert total >= has

            def __str__(self):
                return f"{RC}CN{NC}({BC}{self.has}{NC}/{BC}{self.total}{NC})"

            def __repr__(self):
                return str(self)

            def remove(self, other: "Direct.Category.CtrlNumber"):
                self.has -= other.has
                self.total -= other.total

        def __init__(self, group: "Direct.Group", type_category: "Direct.Category.Type"):
            self.group = group
            self.type = type_category

            self.ctrl_number = Direct.Category.CtrlNumber(0, 0)
            self.requests: list["Request"] = []

        def __str__(self):
            return f"{RC}C{NC}({self.type.name})"

        def __getitem__(self, rating_or_student: Union[int, "Student"]) -> Optional["Request"]:
            if isinstance(rating_or_student, int):
                for request in self.requests:
                    if request.rating == rating_or_student:
                        return request
            else:
                for request in self.requests:
                    if request.student == rating_or_student:
                        return request

        def __hash__(self):
            return hash(self.type)

    def __init__(self, faculty: "Faculty", name: str, form: str, level: str):
        self.faculty = faculty

        self.name = name
        self.form = form
        self.level = level
        self._groups: Set["Direct.Group"] = set()

        self.faculty.add_direct(self)

    def __str__(self):
        return f"{RC}D{NC}({self.name})"

    def __getitem__(self, group_type: "Direct.Group.Type") -> "Direct.Group":
        for group in self._groups:
            if group.type == group_type:
                return group
        group = Direct.Group(self, group_type)
        self._groups.add(group)
        return group

    def __hash__(self):
        return hash(str(self))

    def fullname(self):
        return f"{self.level} {self.form} {self.faculty} {self.name}"

    @classmethod
    def from_name_and_fac(cls, faculty: "Faculty", name: str, form: str, level: str):
        for direct in faculty._directs:
            if direct.name == name and direct.form == form and direct.level == level:
                return direct
        return cls(faculty, name, form, level)


class Request:
    def __init__(self, rating: int, student: "Student", category: "Direct.Category", **options):
        self.rating = rating
        self.student = student
        self.category = category

        self.total_sum = options.pop("total_sum", 0)
        self.marks = options.pop("marks", (0, 0, 0))
        self.original_doc = options.pop("original_doc", False)
        self.consent_enr = options.pop("consent_enr", False)

        self.student.requests.add(self)

    def __str__(self):
        marks = ", ".join(f"{BC}{m}{NC}" for m in self.marks)
        return f"{RC}R{BL_C}({NC}№{BC}{self.rating}{NC} {self.student} m={BC}{self.total_sum}{NC} ({marks}){BL_C}){NC}"

    def __hash__(self):
        return hash(str(self))

    def is_consent(self):
        return self.original_doc or self.consent_enr


class Student:
    ALL: Set["Student"] = set()

    def __init__(self, snils: str):
        self.snils = snils.replace("-", "").replace(" ", "")
        self.name = None

        self.requests: Set["Request"] = set()
        Student.ALL.add(self)

    def __str__(self):
        return f"{RC}S{NC}({GC}{self.name or self.snils}{NC})"

    def __eq__(self, other: "Student"):
        return self.snils == other.snils

    def __ne__(self, other: "Student"):
        return self.snils != other.snils

    def __hash__(self):
        return hash("Student:" + self.snils)

    @classmethod
    def from_snils(cls, snils: str):
        snils = snils.replace("-", "").replace(" ", "")
        for elem in Student.ALL:
            if elem.snils == snils:
                return elem
        return cls(snils)

    def get_consent_req(self) -> Optional["Request"]:
        for req in self.requests:
            if req.is_consent():
                return req
