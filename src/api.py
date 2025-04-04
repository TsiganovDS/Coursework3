import requests

class HHAPI:
    def __init__(self, companies: list):
        self.companies = companies
        self.employers = []
        self.vacancies = []
        self.base_employer_url = "https://api.hh.ru/employers"
        self.base_vacancy_url = "https://api.hh.ru/vacancies"

    def hh_api(self) -> tuple[list[dict], list[dict]]:
        """Получает список вакансий по указанным компаниям."""
        employers_data = []

        for company in self.companies:
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
                            "alternate_url": employer["url"],
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
