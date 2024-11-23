from bs4 import BeautifulSoup
import requests
import urllib.parse
import re
from itertools import islice
import json

url = "https://books.toscrape.com/"
url_book = urllib.parse.urljoin(url, "catalogue/")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
}


def html_download(url):
    """Загружает HTML страницу по указанному URL"""
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")
    return soup if response.status_code == 200 else None


def books_info_scraping(links):
    """Извлекает информацию о книгах по списку ссылок"""
    books = []
    for link in links:
        book = html_download(link)
        if book is not None:
            name = book.find('div', {'class': 'col-sm-6 product_main'}).find_next('h1').text.strip()
            price = float(re.findall(r'\d+\.\d+', book.find('p', {'class': 'price_color'}).text)[0])
            in_stock = int(re.findall(r'\d+', book.find('p', {'class': 'instock availability'}).text.strip())[0])
            upc_row = book.find('th', string='UPC').parent
            upc_value = upc_row.find('td').text
            # Обработка описания книги
            try:
                description_tag = book.find('div', {'id': 'product_description'})
                description_paragraph = description_tag.find_next('p') if description_tag else None
                description = description_paragraph.text.strip() if description_paragraph else "Описание отсутствует"
            except Exception as e:
                description = "Описание отсутствует"
                print(f"Ошибка при извлечении описания: {e}")

            books.append({
                '_id' : upc_value,
                'name': name,
                'price': price,
                'in_stock': in_stock,
                'description': description
            })
    return books


def get_all_books_in_category(category_url):
    """Извлекает все книги из категории с учетом пагинации"""
    books_in_category = []
    page_number = 1
    while True:
        # Строим URL страницы с учетом пагинации
        if page_number == 1:
            page_url = category_url  # Первая страница имеет стандартный URL
        else:
            page_url = f"{category_url.replace('index.html', '')}page-{page_number}.html"  # Следующие страницы добавляют page-{номер}

        print(f"Загружаем страницу: {page_url}")

        page_content = html_download(page_url)
        if page_content is None:
            print(f"Ошибка загрузки страницы {page_url}. Прерываем.")
            break

        # Извлекаем ссылки на книги с текущей страницы
        ul_list = page_content.find_all('article', {'class': 'product_pod'})
        book_links = [
            urllib.parse.urljoin(url_book, article.h3.a.get('href').replace('../../../', ''))
            for article in ul_list
        ]
        books_in_category.extend(books_info_scraping(book_links))

        # Проверяем наличие пагинации, если есть "next", переходим на следующую страницу
        next_button = page_content.find('li', {'class': 'next'})
        if next_button:
            page_number += 1
        else:
            break

    return books_in_category


# Извлекаем ссылки на категории
dict_links = {}
book_content = html_download(url)
if book_content is not None:
    ul_list = book_content.find('ul', {'class': 'nav nav-list'})
    links = ul_list.find_all('a')  # Получаем все ссылки со страницы
    for link in links:  # Получаем категории и ссылки
        text = link.text.strip()
        href = link.get('href')
        full_link = urllib.parse.urljoin(url, href)
        dict_links[text] = full_link

# Извлекаем книги из всех категорий
books = {}
for key, link in islice(dict_links.items(), 1, None):  # Получаем ссылки на все категории, исключая первую
    books[key] = get_all_books_in_category(link)

# Сохранение данных в JSON файл
with open("books_upc.json", "w", encoding="utf-8") as f:
    json.dump(books, f, ensure_ascii=False, indent=4)

print("Данные о книгах успешно сохранены в файл books.json.")
