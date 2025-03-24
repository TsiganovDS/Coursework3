import json

import psycopg2
import requests

from src.api import hh_api

def save_to_json():
    """Функция записи в JSON-файл"""
    employers_data, vacancies = hh_api()
    with open("employers.json", "w", encoding="utf-8") as f:
        employers_to_save = [
            {"id": emp["id"], "name": emp["name"], "url": emp["url"]}
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
                    "url": vac["alternate_url"],
                }
            )
        json.dump(vacancies_to_save, f, indent=4, ensure_ascii=False)


save_to_json()


def drop_tables():
    """Функция удаления таблиц"""
    conn = psycopg2.connect(
        dbname="hh.ru", user="postgres", password="1234", host="localhost"
    )
    cursor = conn.cursor()

    cursor.execute("""
            DROP TABLE IF EXISTS
                vacancies,
                employers
            CASCADE;
        """)

    conn.commit()
    cursor.close()
    conn.close()


def create_tables():
    """Функция создания таблиц"""
    drop_tables()
    conn = psycopg2.connect(
        dbname="hh.ru", user="postgres", password="1234", host="localhost"
    )
    cursor = conn.cursor()

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

    conn.commit()
    cursor.close()
    conn.close()


def insert_data(employers_data, vacancies):
    """Функция заполнения таблиц"""
    conn = psycopg2.connect(
        dbname="hh.ru", user="postgres", password="1234", host="localhost"
    )
    cursor = conn.cursor()

    for employer in employers_data:
        cursor.execute("SELECT id FROM employers WHERE id = %s", (employer["id"],))
        existing_employer = cursor.fetchone()

        if not existing_employer:
            cursor.execute(
                "INSERT INTO employers (name, url, id) VALUES (%s, %s, %s) RETURNING id",
                (employer["name"], employer["url"], employer["id"]),
            )
            employer_id = cursor.fetchone()[0]
        else:
            employer_id = existing_employer[0]

        for vacancy in vacancies:
            if "employer" not in vacancy:
                continue

            if vacancy["employer"]["name"] == employer["name"]:
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
                        vacancy["alternate_url"],
                    ),
                )

    conn.commit()
    cursor.close()
    conn.close()


create_tables()
employers_data, vacancies = hh_api()
insert_data(employers_data, vacancies)
