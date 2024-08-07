import json
import psycopg2
import os

def load_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)

def insert_employers(employers, conn):
    with conn.cursor() as cur:
        for employer_id, employer in employers.items():
            cur.execute("""
                INSERT INTO employers (id, name, description, site_url, alternate_url, vacancies_url, trusted)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                employer_id,
                employer.get('name'),
                employer.get('description'),
                employer.get('site_url'),
                employer.get('alternate_url'),
                employer.get('vacancies_url'),
                employer.get('trusted')
            ))

def insert_vacancies(vacancies, conn):
    with conn.cursor() as cur:
        for vacancy in vacancies:
            cur.execute("""
                INSERT INTO vacancies (id, name, area_id, area_name, published_at, created_at, employer_id, requirement, responsibility, salary, url, alternate_url, employment, schedule)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                vacancy.get('id'),
                vacancy.get('name'),
                vacancy.get('area', {}).get('id'),
                vacancy.get('area', {}).get('name'),
                vacancy.get('published_at'),
                vacancy.get('created_at'),
                vacancy.get('employer', {}).get('id'),
                vacancy.get('snippet', {}).get('requirement'),
                vacancy.get('snippet', {}).get('responsibility'),
                json.dumps(vacancy.get('salary')),
                vacancy.get('url'),
                vacancy.get('alternate_url'),
                vacancy.get('employment', {}).get('name'),
                vacancy.get('schedule', {}).get('name')
            ))

def main():
    current_dir = os.path.dirname(__file__)
    employers_path = os.path.join(current_dir, 'data', 'employers.json')
    vacancies_path = os.path.join(current_dir, 'data', 'vacancies.json')

    employers = load_json(employers_path)
    vacancies = load_json(vacancies_path)

    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='12345',
        host='localhost',
        port='5432'
    )

    try:
        insert_employers(employers, conn)
        insert_vacancies(vacancies, conn)
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == '__main__':
    main()
