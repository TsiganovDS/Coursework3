import json

import psycopg2


class DBManager:
    def __init__(self, dbname, user, password, host):
        self.host = host
        self.password = password
        self.user = user
        self.dbname = dbname
        self.conn = psycopg2.connect(
        dbname="hh.ru", user="postgres", password="1234", host="localhost"
    )


    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT e.name, COUNT(v.id)
                FROM employers e
                LEFT JOIN vacancies v ON e.id = v.employer_id
                GROUP BY e.id;
            """
            )
            return cursor.fetchall()

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    e.name as company,
                    v.title as vacancy,
                    v.salary_from,
                    v.salary_to,
                    v.url
                FROM vacancies v
                JOIN employers e ON v.employer_id = e.id;
            """
            )

            columns = [desc[0] for desc in cursor.description]
            result = [dict(zip(columns, row)) for row in cursor.fetchall()]

            return json.dumps(result, ensure_ascii=False, indent=4)

    def get_avg_salary(self):
        """Считает среднюю зарплату с учётом всех возможных вариантов указания зарплат."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT AVG(
                    CASE
                        WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN (salary_from + salary_to)/2
                        WHEN salary_from IS NOT NULL THEN salary_from
                        WHEN salary_to IS NOT NULL THEN salary_to
                    END
                )
                FROM vacancies;
            """
            )
            result = cursor.fetchone()[0]
            return round(result, 2) if result else 0

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        avg_salary = self.get_avg_salary()
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT e.name, v.title, v.salary_from, v.salary_to, v.url
                FROM vacancies v
                JOIN employers e ON v.employer_id = e.id
                WHERE (salary_from + salary_to) / 2 > %s;
            """,
                (avg_salary,),
            )
            return cursor.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT e.name, v.title, v.salary_from, v.salary_to, v.url
                FROM vacancies v
                JOIN employers e ON v.employer_id = e.id
                WHERE v.title ILIKE %s;
            """,
                (f"%{keyword}%",),
            )
            return cursor.fetchall()

    def close(self):
        self.conn.close()


db_manager = DBManager(
        dbname="hh.ru", user="postgres", password="1234", host="localhost"
    )

print(db_manager.get_avg_salary())
print(db_manager.get_all_vacancies())
print(db_manager.get_companies_and_vacancies_count())
print(db_manager.get_vacancies_with_higher_salary())
print(db_manager.get_vacancies_with_keyword("Продавец"))
