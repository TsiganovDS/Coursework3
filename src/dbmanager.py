import json
from typing import List, Tuple, Any

import psycopg2

from src.vacancies import DB_CONFIG


class DBManager:
    def __init__(self, dbname, user, password, host):
        self.host = host
        self.password = password
        self.user = user
        self.dbname = dbname
        self.conn = psycopg2.connect(**DB_CONFIG)

    def get_companies_and_vacancies_count(self) -> list[tuple[Any, ...]]:
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

    def get_all_vacancies(self) -> str:
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

    def get_avg_salary(self) -> float:
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

    def get_vacancies_with_higher_salary(self) -> list[tuple[Any, ...]]:
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

    def get_vacancies_with_keyword(self, keyword: str) -> list[tuple[Any, ...]]:
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

    def close(self) -> None:
        self.conn.close()
