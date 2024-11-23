import json
import clickhouse_connect

client = clickhouse_connect.get_client (host = 'pyo6gdhryr.europe-west4.gcp.clickhouse.cloud',
                 user = '**********',
                 secure = True,
                 password = '********')

print("Result:", client.query("SELECT 1").result_set[0][0])

# Выполняем запрос для получения списка всех баз данных
result = client.query("SHOW DATABASES").result_set

# Выводим все базы данных
for database in result:
    print(database[0])

# Выполняем запрос для получения списка всех баз данных
result = client.query("SHOW TABLES").result_set

# Выводим все базы данных
for database in result:
    print(database[0])


# Открываем файл JSON и загружаем данные
with open('books_upc.json', 'r', encoding='utf-8') as f:
    data = json.load(f)


# Преобразуем данные в формат, подходящий для вставки
books = []
for category, items in data.items():
    for item in items:
        # Обрабатываем строки для безопасного вставления в базу данных
        def escape_string(value):
            if value is None:
                return ""
            return value.replace("'", "''").replace('"', '""').replace("\n", " ").replace("\r", " ")

        books.append((
            item["_id"],
            escape_string(item["name"]),
            item["price"],
            item["in_stock"],
            escape_string(item.get("description", ""))
        ))

# Формируем запрос на вставку
insert_query = """
    INSERT INTO default.books (_id, name, price, in_stock, description)
    VALUES
"""

# Добавляем данные в запрос
insert_query += ', '.join([f"('{book[0]}', '{book[1]}', {book[2]}, {book[3]}, '{book[4]}')" for book in books])


# Выполняем запрос на вставку данных
client.command(insert_query)