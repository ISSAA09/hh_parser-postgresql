import json
from src.utils import get_vacancies_by_employer, save_data_to_database, create_database
from src.dbmanager import DBManager

with open('employers_id.json', 'r', encoding="utf-8") as json_file:
    employer_ids = json.load(json_file)

if __name__ == "__main__":

    print("Привет,хотите подключить/создать БД ?\nЕсли да, введите название БД")
    database_name = input().lower()
    create_database(database_name)
    print(f"БД '{database_name}' успешно подключена")

    print(f"Хотите загрузить данные о крупных компаниях и их вакансиях с сайта hh.ru в БД '{database_name}'?")
    user_answer = input('Да или нет? ').lower()
    print('Выполняется процесс...')

    if user_answer == 'да':
        all_vacancies = []
        for id_emp in employer_ids['largest_companies']:
            all_vacancies.append(get_vacancies_by_employer(id_emp))
        save_data_to_database(all_vacancies, database_name)
        print("Данные успешно загружены")
        while True:
            print("Хотите:\n"
                  "1. Получить список всех компаний и количество вакансий у каждой компании?\n"
                  "2. Получить список всех вакансий с указанием названия компании, названия вакансии и зарплаты "
                  "и ссылки на вакансию?\n"
                  "3. Получить среднюю зарплату по вакансиям?\n"
                  "4. Получить список всех вакансий, у которых зарплата выше средней по всем вакансиям?\n"
                  "5. Получить список всех вакансий, в названии которых содержится переданное вами слово?\n")
            print("Введите число соответсвующее вашему запросу")
            input_number = input()
            dbm = DBManager(database_name, "postgres", "8810533", "localhost", "5432")
            if input_number == '1':
                print(dbm.get_companies_and_vacancies_count())
            elif input_number == '2':
                print(dbm.get_all_vacancies())
            elif input_number == '3':
                print(dbm.get_avg_salary())
            elif input_number == '4':
                print(dbm.get_vacancies_with_higher_salary())
            elif input_number == '5':
                print('Введите ключевое слово')
                keyword_input = input()
                print(dbm.get_vacancies_with_keyword(keyword_input))
            else:
                print('Неверный выбор')
            print("Хотите продолжить?")
            if input().lower() != 'да':
                print('Всего доброго!')
                break
    else:
        print('Всего доброго!')

