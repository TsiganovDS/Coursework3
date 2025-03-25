from src.api import hh_api
from src.dbmanager import DBManager
from src.vacancies import DB_CONFIG, create_tables, insert_data


def user_interaction():
    """Интерактивное меню для работы с базой вакансий"""

    db = DBManager(**DB_CONFIG)

    while True:
        print("\n" + "=" * 50)
        print("[1] Создать таблицы (ОСТОРОЖНО! Удалит старые данные)")
        print("[2] Заполнить таблицы данными")
        print("[3] Список компаний и количество вакансий")
        print("[4] Все вакансии")
        print("[5] Средняя зарплата")
        print("[6] Вакансии с зарплатой выше средней")
        print("[7] Поиск по ключевому слову")
        print("[0] Выход")
        choice = input("Выберите действие: ").strip()

        if choice == "1":
            confirm = input("УДАЛИТ ВСЕ ДАННЫЕ! Продолжить? (Y/N): ").strip().lower()
            if confirm == "y":
                create_tables()
                print("Таблицы успешно созданы!")

        elif choice == "2":
            if input("Загрузить данные? (Y/N): ").strip().lower() == "y":
                employers_data, vacancies = hh_api()
                insert_data(employers_data, vacancies)
                print("Данные загружены!")

        elif choice == "3":
            print("\nКомпании и вакансии:")
            for company, count in db.get_companies_and_vacancies_count():
                print(f"- {company}: {count} вакансий")

        elif choice == "4":
            print("\nВсе вакансии:")
            vacancies_json = db.get_all_vacancies()
            print(vacancies_json)

        elif choice == "5":
            avg = db.get_avg_salary()
            print(f"\nСредняя зарплата: {avg} руб.")

        elif choice == "6":
            print("\nВакансии с зарплатой выше средней:")
            for vac in db.get_vacancies_with_higher_salary():
                print(f"- {vac[1]} ({vac[0]}): от {vac[2]} до {vac[3]} руб.")

        elif choice == "7":
            keyword = input("\nВведите ключевое слово: ").strip()
            print(f"Результаты поиска '{keyword}':")
            for vac in db.get_vacancies_with_keyword(keyword):
                print(f"- {vac[1]} ({vac[0]}): {vac[4]}")

        elif choice == "0":
            db.close()
            print("\nРабота завершена.")
            break

        else:
            print("\nНет такого варианта, давай ещё раз!")


user_interaction()
