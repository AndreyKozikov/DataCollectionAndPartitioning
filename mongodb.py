from pymongo import MongoClient
import ijson  # Для парсинга json файла частями
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from decimal import Decimal
from bson import ObjectId


HOST = 'localhost'
PORT = 27017
FILE_NAME = 'books_upc.json'


def connection_checking(client):
    try:
        # Команда ping проверяет соединение с сервером
        client.admin.command('ping')
        print("Соединение с MongoDB успешно установлено!")
    except ConnectionFailure:
        print("Не удалось подключиться к MongoDB")
        return False
    return True


def insert_collection(collection, data):
    duplicated_count = 0
    for object in data:
        try:
            collection.insert_one(object)
        except DuplicateKeyError:
            duplicated_count += 1
    return duplicated_count


def reading_from_file(file, db, collection):
    duplicated = 0
    buffer = []
    buffer_size = 1000
    current_category = None
    # Открываем JSON файл в бинарном режиме ('rb').
    # Создаем парсер ijson, который будет читать файл потоково.
    with open(file, 'rb') as f:
        parser = ijson.parse(f)
        # ijson.parse() возвращает кортежи (prefix, event, value):
        # prefix: путь к текущему элементу в структуре JSON.
        # event: тип события (начало объекта, конец объекта, значение и т.д.).
        # value: значение текущего элемента.
        for prefix, event, value in parser:
            # Начало объекта
            if prefix == '' and event == 'map_key':
                if current_category and buffer:
                    for book in buffer:
                        book['category'] = current_category
                    buffer = sanitize_data(buffer)  # Преобразуем данные перед вставкой
                    duplicated += insert_collection(collection, buffer)
                    buffer = []
                current_category = value
                note = {}
            elif prefix.endswith('.item')and event == 'start_map':
                # Начало нового объекта книги
                note = {}
            # Конец объекта
            elif prefix.endswith('.item') and event == 'end_map':

                buffer.append(note)
                if len(buffer) >= buffer_size:
                    for book in buffer:
                        book['category'] = current_category
                    buffer = sanitize_data(buffer)  # Преобразуем данные перед вставкой
                    duplicated += insert_collection(collection, buffer)
                    buffer = []
            elif event in {'string', 'number'}:
                # Заполнение данных книги
                key = prefix.split('.')[-1]
                note[key] = value

        if current_category and buffer:
            for book in buffer:
                book['category'] = current_category
            buffer = sanitize_data(buffer)  # Преобразуем данные перед вставкой
            duplicated += insert_collection(collection, buffer)

def sanitize_data(data):
    """
    Преобразует значения типа decimal.Decimal в float, если они есть в данных.
    """
    for item in data:
        for key, value in item.items():
            if isinstance(value, Decimal):
                item[key] = float(value)
    return data

if __name__ == '__main__':
    client = MongoClient(HOST, PORT)
    if connection_checking(client):
        db = client['library']
        collection = db['books']
        collection.drop()
        reading_from_file(FILE_NAME, db, collection)

        # Вернем все книги в категории Crime
        print("Вернем все книги в категории Crime")
        result = collection.find({'category' : 'Crime'})
        for doc in result:
            print(doc)
        print()

        # Вернем все книги в категории Crime или Erotica
        print("Вернем все книги в категории Crime или Erotica")
        result = collection.find({
            '$or' : [
                {'category' : 'Crime'},
                {'category' : 'Erotica'}
            ]})
        for doc in result:
            print(doc)

        print()
        print("Вернем все книги c ценой от 10 до 15")
        result = collection.find({'price' : {'$gte' : 10, '$lte' : 15}})
        for doc in result:
            print(doc)

        print()
        print("Вернем все книги c ценой от 10 до 25 в категории 'Self Help' или 'Contemporary' ")
        result = collection.find({
            '$and' : [{'price': {'$gte': 10, '$lte': 25}},
                      {'$or' : [
                          {'category' : 'Self Help'},
                          {'category': 'Contemporary'}
                      ]}]
        })
        for doc in result:
            print(doc)

        print()
        print("Вернем все книги где в описании содержится child без учета регистра")
        result = collection.find({'description': {'$regex': 'child', '$options': 'i'}})
        for doc in result:
            print(doc)

        print()
        print("Вернем все книги, у которых нет описания")
        result = collection.find({
            'description': {'$exists': False}
        })
        for doc in result:
            print(doc)

        print()
        print("Количество книг в  каждой категории")
        result = collection.aggregate([
            {"$group": {"_id": "$category", "total_books": {"$sum": 1}}}
        ])
        for doc in result:
            print(doc)

        print()
        # Установим цену всех книг равной 16, если цена меньше 15
        count = collection.count_documents({'price': {'$lt' : 15}})
        print(f"Количество книг с ценой меньше 15: {count}")
        collection.update_many({'price': {'$lt' : 15}},
                               {'$set' : {'price' : 16}})
        count = collection.count_documents({'price': {'$lt': 15}})
        print(f"Количество книг с ценой меньше 15: {count}")
        count = collection.count_documents({'price': {'$eq': 16}})
        print(f"Количество книг с ценой 16: {count}")

        print()
        # Удалим книги в категории Biography
        count = collection.count_documents({'category': 'Biography'})
        print(f"Количество книг в категории 'Biography' до удаления: {count}")
        collection.delete_many({'category' : 'Biography'})
        count = collection.count_documents({'category': 'Biography'})
        print(f"Количество книг в категории 'Biography' после удаления: {count}")

    else:
        exit(1)
