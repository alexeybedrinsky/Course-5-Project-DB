import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="12345"
    )
    print("Соединение установлено")
    conn.close()
except Exception as e:
    print(f"Ошибка подключения: {e}")
