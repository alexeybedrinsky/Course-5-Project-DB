import psycopg2


class DBManager:
    def __init__(self, dbname, user, password, host, port):
        try:
            self.conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
            print("Successfully connected to the database")
        except Exception as e:
            print(f"Error connecting to the database: {e}")

    def get_companies_and_vacancies_count(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT e.name, COUNT(v.id) as vacancies_count
                    FROM employers e
                    JOIN vacancies v ON e.id = v.employer_id
                    GROUP BY e.name;
                """)
                return cur.fetchall()
        except Exception as e:
            print(f"Error fetching companies and vacancies count: {e}")

    def get_all_vacancies(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT v.name, e.name as employer_name, v.salary, v.url
                    FROM vacancies v
                    JOIN employers e ON v.employer_id = e.id;
                """)
                return cur.fetchall()
        except Exception as e:
            print(f"Error fetching all vacancies: {e}")

    def get_avg_salary(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT AVG((salary::json->>'from')::INTEGER) as avg_salary
                    FROM vacancies
                    WHERE salary IS NOT NULL;
                """)
                return cur.fetchone()
        except Exception as e:
            print(f"Error fetching average salary: {e}")

    def get_vacancies_with_higher_salary(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT v.name, e.name as employer_name, v.salary, v.url
                    FROM vacancies v
                    JOIN employers e ON v.employer_id = e.id
                    WHERE (salary::json->>'from')::INTEGER > (SELECT AVG((salary::json->>'from')::INTEGER) FROM vacancies WHERE salary IS NOT NULL);
                """)
                return cur.fetchall()
        except Exception as e:
            print(f"Error fetching vacancies with higher salary: {e}")

    def get_vacancies_with_keyword(self, keyword):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT v.name, e.name as employer_name, v.salary, v.url
                    FROM vacancies v
                    JOIN employers e ON v.employer_id = e.id
                    WHERE v.name ILIKE %s
                    OR (v.requirement IS NOT NULL AND v.requirement ILIKE %s)
                    OR (v.responsibility IS NOT NULL AND v.responsibility ILIKE %s);
                """, (f'%{keyword}%', f'%{keyword}%', f'%{keyword}%'))
                return cur.fetchall()
        except Exception as e:
            print(f"Error fetching vacancies with keyword '{keyword}': {e}")

    def close(self):
        self.conn.close()
        print("Database connection closed")


if __name__ == "__main__":
    db_manager = DBManager('postgres', 'postgres', '12345', 'localhost', '5432')

    print("Companies and vacancies count:")
    print(db_manager.get_companies_and_vacancies_count())

    print("\nAll vacancies:")
    print(db_manager.get_all_vacancies())

    print("\nAverage salary:")
    print(db_manager.get_avg_salary())

    print("\nVacancies with higher than average salary:")
    print(db_manager.get_vacancies_with_higher_salary())

    print("\nVacancies with keyword 'Python':")
    print(db_manager.get_vacancies_with_keyword('Python'))

    db_manager.close()
