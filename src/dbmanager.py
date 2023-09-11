import psycopg2


class DBManager:
    """
        Класс для работы с данными в БД
    """
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def connect(self):
        """
            Подключение к БД
        """
        conn = psycopg2.connect(dbname=self.dbname, user=self.user,
                                password=self.password, host=self.host, port=self.port)
        return conn

    def get_companies_and_vacancies_count(self):
        """
            Получает список всех компаний и количество вакансий у каждой компании.
        """
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT employers.employer_name, COUNT(*) FROM employers JOIN vacancies USING(employer_id) GROUP BY "
            "employers.employer_name")
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result

    def get_all_vacancies(self):
        """
             Получает список всех вакансий с указанием названия компании,
             названия вакансии и зарплаты и ссылки на вакансию.
        """
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("SELECT employers.employer_name, vacancies.vacancy_name, salary_from, salary_to, "
                    "vacancies.vacancy_url "
                    "FROM employers "
                    "JOIN vacancies USING(employer_id)")
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result

    def get_avg_salary(self):
        """
            Получает среднюю зарплату по вакансиям.
        """
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("SELECT AVG(salary_from) FROM vacancies")
        result = cur.fetchone()[0]
        cur.close()
        conn.close()
        return result

    def get_vacancies_with_higher_salary(self):
        """
            Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
        """
        avg_salary = self.get_avg_salary()
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("SELECT employer_name, vacancy_name, salary_from, vacancy_url FROM vacancies "
                    "JOIN employers USING(employer_id)"
                    "WHERE salary_from > %s", (avg_salary,))
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result

    def get_vacancies_with_keyword(self, keyword):
        """
            Получает список всех вакансий, в названии которых содержатся переданные в метод слова
        """
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("SELECT * FROM vacancies "
                    "WHERE vacancy_name ILIKE %s", ('%' + keyword + '%',))
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result
