import psycopg2
import requests


def get_vacancies_by_employer(employer_id: int):
    """
        Получает данные о работодателях и их вакансиях с сайта hh.ru.
    """
    collect_employers = []
    collect_vacancies = []

    url_employer = f"https://api.hh.ru/employers/{employer_id}"
    response_employer = requests.get(url_employer)
    if response_employer.status_code == 200:
        employer_data = response_employer.json()

        collect_employers.append({
            'employer_id': employer_data['id'],
            'employer_name': employer_data['name'],
            'employer_url': employer_data['alternate_url'],
            'open_vacancies': employer_data['open_vacancies']
        })

    page = 0
    per_page = 100

    while True:
        url_vacancies = "https://api.hh.ru/vacancies"
        params = {'employer_id': employer_id, "page": page, "per_page": per_page, 'only_with_salary': True}
        response_vacancies = requests.get(url_vacancies, params=params)
        vacancies_data = response_vacancies.json()

        if not vacancies_data.get('items'):
            break
        for vacancy in vacancies_data.get('items'):
            collect_vacancies.append(
                {
                    'employer_id': vacancy['employer']['id'],
                    'vacancy_id': vacancy['id'],
                    'employer_name': vacancy['employer']['name'],
                    'vacancy_name': vacancy['name'],
                    'salary_from': vacancy.get('salary', {}).get('from', 'N/A'),
                    'salary_to': vacancy.get('salary', {}).get('to', 'N/A'),
                    'vacancy_url': vacancy['alternate_url']
                }
            )
        page += 1
        if page > vacancies_data["pages"]:
            break
    return {
        "employers": collect_employers,
        "vacancies": collect_vacancies
    }


def create_database(database_name: str):
    """
        Создание БД и таблицы для хранения полученных данных о работодателях и их вакансиях.
    """

    conn = psycopg2.connect(dbname="postgres", user="postgres",
                            password="8810533",
                            host="localhost",
                            port="5432"
                            )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {database_name}")
    cur.execute(f"CREATE DATABASE {database_name}")

    conn.close()

    conn = psycopg2.connect(dbname=database_name,
                            user="postgres",
                            password="8810533",
                            host="localhost",
                            port="5432")

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE employers (
                employer_id int PRIMARY KEY,
                employer_name VARCHAR(255) NOT NULL,
                open_vacancies INTEGER,
                employer_url TEXT
            )
        """)
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE vacancies (
                vacancy_id int PRIMARY KEY,
                employer_id int,
                vacancy_name VARCHAR NOT NULL,
                salary_from int,
                salary_to int,
                vacancy_url TEXT
            )
        """)

    conn.commit()
    conn.close()


def save_data_to_database(data, database_name):
    """
        Заполняет созданные в БД таблицы данными о работодателях и их вакансиях
    """

    conn = psycopg2.connect(dbname=database_name,
                            user="postgres",
                            password="8810533",
                            host="localhost",
                            port="5432")

    with conn.cursor() as cur:
        for d in data:
            for employer in d['employers']:
                cur.execute(
                    """
                   INSERT INTO employers (employer_id, employer_name, open_vacancies, employer_url)
                   VALUES (%s, %s, %s,%s)
                   """,
                    (employer['employer_id'],
                     employer['employer_name'],
                     employer['open_vacancies'],
                     employer['employer_url'])
                )

        for v in data:
            vacancy_list = []
            for vacancy in v['vacancies']:
                vacancy_list.append(vacancy)
            for z in vacancy_list:
                cur.execute(
                    """
                   INSERT INTO vacancies (vacancy_id, employer_id, vacancy_name, salary_from,salary_to,vacancy_url)
                   VALUES (%s, %s, %s, %s,%s,%s)
                   """,
                    (z['vacancy_id'], z["employer_id"], z['vacancy_name'],
                     z['salary_from'],
                     z['salary_to'], z['vacancy_url'])
                )
    conn.commit()
    conn.close()
