import requests


def hh_api():
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

    employers_data = []

    for company in companies:
        response = requests.get(
            f"https://api.hh.ru/employers?text={company}&only_with_vacancies=true"
        )
        if response.status_code == 200:
            employers = response.json()["items"]
            if employers:
                employer = employers[0]
                employers_data.append(
                    {
                        "id": employer["id"],
                        "name": employer["name"],
                        "url": employer["alternate_url"],
                    }
                )

    vacancies = []

    for employer in employers_data:
        response = requests.get(
            f'https://api.hh.ru/vacancies?employer_id={employer["id"]}'
        )
        if response.status_code == 200:
            data = response.json()
            vacancies.extend(data["items"])
    return employers_data, vacancies
