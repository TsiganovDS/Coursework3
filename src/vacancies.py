import json
import os

import psycopg2
from dotenv import load_dotenv

from src.api import HHAPI

load_dotenv("../.env")
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
}

def drop_tables() -> None:
    """Функция удаления таблиц"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                 DROP TABLE IF EXISTS vacancies, employers CASCADE;
                 """
            )


def create_tables() -> None:
    """Функция создания таблиц"""
    drop_tables()
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS employers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                url VARCHAR(255)
            );
            """
            )

            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS vacancies (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                employer_id INTEGER REFERENCES employers(id),
                salary_from INTEGER,
                salary_to INTEGER,
                url VARCHAR(255)
            );
            """
            )


def insert_data(employers_data: list[dict], vacancies: list[dict]) -> None:
    """Функция заполнения таблиц"""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            for employer in employers_data:
                cursor.execute(
                    "INSERT INTO employers (id, name, url) "
                    "VALUES (%s, %s, %s) "
                    "ON CONFLICT (id) DO NOTHING "
                    "RETURNING id",
                    (employer["id"], employer["name"], employer["alternate_url"]),
                )
                employer_id = employer["id"]

                for vacancy in vacancies:
                    if vacancy.get("employer", {}).get("name") != employer["name"]:
                        continue

                    salary_data = vacancy.get("salary") or {}
                    cursor.execute(
                        """INSERT INTO vacancies
                        (title, employer_id, salary_from, salary_to, url)
                        VALUES (%s, %s, %s, %s, %s)""",
                        (
                            vacancy["name"],
                            employer_id,
                            salary_data.get("from"),
                            salary_data.get("to"),
                            vacancy["url"],
                        ),
                    )
            conn.commit()


def save_to_json() -> None:
    """Функция записи в JSON-файл"""
    companies = [
        "Сибирский цирюльник",
        "Нефтяночка42",
        "СЗМК",
        "Т-Банк",
        "ТехноОпт",
        "2ГИС",
        "Додо Пицца",
        "Группа компаний РТС",
        "Газпром нефть",
        "Альфа-банк",
    ]
    api = HHAPI(companies)
    employers_data, vacancies = api.hh_api()
    with open("employers.json", "w", encoding="utf-8") as f:
        employers_to_save = [
            {"id": emp["id"], "name": emp["name"], "url": emp.get("url", "N/A")}
            for emp in employers_data
            if emp["id"] is not None
        ]
        json.dump(employers_to_save, f, indent=2, ensure_ascii=False)

    with open("vacancies.json", "w", encoding="utf-8") as f:
        vacancies_to_save = []
        for vac in vacancies:
            vacancies_to_save.append(
                {
                    "id": vac["id"],
                    "title": vac["name"],
                    "employer_id": vac["employer"]["id"],
                    "salary_from": (vac.get("salary") or {}).get("from"),
                    "salary_to": (vac.get("salary") or {}).get("to"),
                    "url": vac["url"],
                }
            )
        json.dump(vacancies_to_save, f, indent=4, ensure_ascii=False)
