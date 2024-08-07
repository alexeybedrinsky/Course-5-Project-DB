import requests
import json
import os

def fetch_vacancies(query, area, page, per_page):
    url = 'https://api.hh.ru/vacancies'
    params = {
        'text': query,
        'area': area,
        'page': page,
        'per_page': per_page
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json().get('items', [])
    else:
        print(f"Ошибка при выполнении запроса: {response.status_code}")
        return []

def fetch_employers(vacancies):
    employers = {}
    for vacancy in vacancies:
        employer_id = vacancy.get('employer', {}).get('id')
        if employer_id and employer_id not in employers:
            url = f'https://api.hh.ru/employers/{employer_id}'
            response = requests.get(url)
            if response.status_code == 200:
                employers[employer_id] = response.json()
            else:
                print(f"Ошибка при получении данных о работодателе: {response.status_code}")
    return employers

def save_to_json(data, filename):
    current_dir = os.path.dirname(__file__)
    json_path = os.path.join(current_dir, filename)
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Данные успешно записаны в файл {json_path}")

# Основной процесс
if __name__ == "__main__":
    # Получение вакансий
    vacancies = fetch_vacancies('Python developer', 1, 0, 10)
    # Получение информации о работодателях
    employers = fetch_employers(vacancies)

    # Сохранение вакансий и работодателей в отдельные файлы JSON
    save_to_json(vacancies, 'vacancies.json')
    save_to_json(employers, 'employers.json')
