import unittest
import applicant_service
import psycopg2.extensions


class GetUserGroupsCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con: psycopg2.extensions.connection = psycopg2.connect(
            host="localhost", database="postgres", user="postgres", password="rooter")

    def setUp(self) -> None:
        self.cur: psycopg2.extensions.cursor = self.con.cursor()

    def test_work(self):
        self.assertEqual([
            {
                'name': 'Институт компьютерных наук и технологий (Очно Бюджет) '
                        'Информационные системы и технологии',
                'university': 'ПГНИУ (г. Пермь)',
                'ctrl_number': 58,
                'rating': 227,
                'group_id': 181,
                'direct_id': 67},
            {
                'name': 'Институт компьютерных наук и технологий (Очно Бюджет) '
                        'Прикладная математика и информатика, программа широкого профиля',
                'university': 'ПГНИУ (г. Пермь)',
                'ctrl_number': 136,
                'rating': 219,
                'group_id': 186,
                'direct_id': 68},
            {
                'name': 'Институт компьютерных наук и технологий (Очно Бюджет) '
                        'Прикладная математика и информатика, профиль Инженерия программного обеспечения',
                'university': 'ПГНИУ (г. Пермь)',
                'ctrl_number': 28,
                'rating': 222,
                'group_id': 190,
                'direct_id': 69},
            {
                'name': 'Институт компьютерных наук и технологий (Очно Бюджет) '
                        'Фундаментальная информатика и информационные технологии',
                'university': 'ПГНИУ (г. Пермь)',
                'ctrl_number': 56,
                'rating': 198,
                'group_id': 193,
                'direct_id': 70},
            {
                'name': 'Механико-математический (Очно Бюджет) Компьютерная безопасность',
                'university': 'ПГНИУ (г. Пермь)',
                'ctrl_number': 28,
                'rating': 139,
                'group_id': 226,
                'direct_id': 79
            }], applicant_service.get_user_groups("16266121755", self.cur)
        )

        self.assertEqual([{
            'name': 'Биологический (Очно Бюджет) Экология',
            'university': 'ПГНИУ (г. Пермь)', 'ctrl_number': 3, 'rating': 1, 'group_id': 4, 'direct_id': 3
        }], applicant_service.get_user_groups("13867938726", self.cur))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.con.close()


class CalcStatDirectCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con: psycopg2.extensions.connection = psycopg2.connect(
            host="localhost", database="postgres", user="postgres", password="rooter")

    def setUp(self) -> None:
        self.cur: psycopg2.extensions.cursor = self.con.cursor()

    def test_has_places(self):
        self.assertEqual(
            {'real_rating': 2, 'consent': 3, 'ctrl_number': 20, 'competition': 0.12},
            applicant_service.calculate_statistic_direct(7, 3, 2, 20)
        )

    def test_very_many_consents(self):
        self.assertEqual(
            {'real_rating': 19, 'consent': 21, 'ctrl_number': 20, 'competition': 'inf'},
            applicant_service.calculate_statistic_direct(40, 21, 0, 20),
        )

    def test_not_have_one_place(self):
        self.assertEqual(
            {'real_rating': 1, 'consent': 20, 'ctrl_number': 20, 'competition': 'inf'},
            applicant_service.calculate_statistic_direct(21, 20, 0, 20),
        )

    def test_has_places_anyway(self):
        self.assertEqual(
            {'real_rating': 5, 'consent': 3, 'ctrl_number': 20, 'competition': 0.29},
            applicant_service.calculate_statistic_direct(10, 3, 2, 20))

    def test_with_one_place(self):
        self.assertEqual(
            {'real_rating': 1, 'consent': 19, 'ctrl_number': 20, 'competition': 1.0},
            applicant_service.calculate_statistic_direct(20, 19, 0, 20),
        )

    def test_without_places(self):
        self.assertEqual(
            {'real_rating': 1, 'consent': 29, 'ctrl_number': 20, 'competition': 'inf'},
            applicant_service.calculate_statistic_direct(40, 29, 10, 20),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.con.close()


class GetUserRealRatingCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con: psycopg2.extensions.connection = psycopg2.connect(
            host="localhost", database="postgres", user="postgres", password="rooter")

    def setUp(self) -> None:
        self.cur: psycopg2.extensions.cursor = self.con.cursor()

    def test_work(self):
        self.assertEqual(
            {'ПГНИУ (г. Пермь)': {181: {'competition': 3.23,
                                        'consent': 11,
                                        'ctrl_number': 58,
                                        'direct_id': 67,
                                        'group_id': 181,
                                        'name': 'Институт компьютерных наук и '
                                                'технологий (Очно Бюджет) '
                                                'Информационные системы и '
                                                'технологии',
                                        'rating': 227,
                                        'real_rating': 152,
                                        'university': 'ПГНИУ (г. Пермь)'},
                                  186: {'competition': 1.32,
                                        'consent': 36,
                                        'ctrl_number': 136,
                                        'direct_id': 68,
                                        'group_id': 186,
                                        'name': 'Институт компьютерных наук и '
                                                'технологий (Очно Бюджет) '
                                                'Прикладная математика и '
                                                'информатика, программа '
                                                'широкого профиля',
                                        'rating': 219,
                                        'real_rating': 132,
                                        'university': 'ПГНИУ (г. Пермь)'},
                                  190: {'competition': 'inf',
                                        'consent': 28,
                                        'ctrl_number': 28,
                                        'direct_id': 69,
                                        'group_id': 190,
                                        'name': 'Институт компьютерных наук и '
                                                'технологий (Очно Бюджет) '
                                                'Прикладная математика и '
                                                'информатика, профиль '
                                                'Инженерия программного '
                                                'обеспечения',
                                        'rating': 222,
                                        'real_rating': 142,
                                        'university': 'ПГНИУ (г. Пермь)'},
                                  193: {'competition': 2.87,
                                        'consent': 10,
                                        'ctrl_number': 56,
                                        'direct_id': 70,
                                        'group_id': 193,
                                        'name': 'Институт компьютерных наук и '
                                                'технологий (Очно Бюджет) '
                                                'Фундаментальная информатика и '
                                                'информационные технологии',
                                        'rating': 198,
                                        'real_rating': 132,
                                        'university': 'ПГНИУ (г. Пермь)'},
                                  226: {'competition': 6.27,
                                        'consent': 13,
                                        'ctrl_number': 28,
                                        'direct_id': 79,
                                        'group_id': 226,
                                        'name': 'Механико-математический (Очно '
                                                'Бюджет) Компьютерная '
                                                'безопасность',
                                        'rating': 139,
                                        'real_rating': 94,
                                        'university': 'ПГНИУ (г. Пермь)'}}},
            applicant_service.get_user_real_rating("16266121755", self.cur)
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.con.close()


class CurPassingScoreCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con: psycopg2.extensions.connection = psycopg2.connect(
            host="localhost", database="postgres", user="postgres", password="rooter")

    def setUp(self) -> None:
        self.cur: psycopg2.extensions.cursor = self.con.cursor()

    def test_ctrl_more_count(self):
        self.assertEqual(-1,
                         applicant_service.get_current_passing_score(177, "Бюджет", "Общий конкурс", self.cur))

    def test_ctrl_equal_count(self):
        self.assertEqual(80,
                         applicant_service.get_current_passing_score(153, "Бюджет", "Общий конкурс", self.cur))

    def test_count_more_ctrl(self):
        self.assertEqual(256,
                         applicant_service.get_current_passing_score(105, "Бюджет", "Общий конкурс", self.cur))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.con.close()


if __name__ == '__main__':
    unittest.main()
