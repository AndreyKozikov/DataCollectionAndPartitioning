from lxml import html
import requests
import re
import csv

base_link = "https://blackterminal.com"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0"
                  " YaBrowser/24.10.0.0 Safari/537.36"
}


def scrape_page_data(url):
    response = requests.get(url, headers=headers)
    response.encoding = 'utf-8'
    if response.status_code == 200:
        html_tree = html.fromstring(response.content)
        rows = html_tree.xpath("//div[@id='w7-container']/table/tbody/tr")
        data = []
        # Получаем ссылку на следующую страницу для пагинации
        next_page = html_tree.xpath("//ul[@class='pagination']/li[contains(@class, 'page-item next')]/a/@href")
        # Проверка на наличие элемента с tabindex на следующей странице
        tabindex = bool(
            html_tree.xpath("//ul[@class='pagination']/li[contains(@class, 'page-item next')]/a[@tabindex]"))
        # Перебор всех строк таблицы для извлечения данных
        for row in rows:
            try:
                first_column = row.xpath(".//a/text()")
                data.append({
                    'name': first_column[0],
                    'nominal': split_price(sanitize_data(row.xpath(".//td[@data-col-seq='1']/text()")[0])),
                    'price': float(sanitize_data(row.xpath(".//td[@data-col-seq='2']/text()")[0][:-1].replace(',', ''))),
                    'nkd': split_price(sanitize_data(row.xpath(".//td[@data-col-seq='3']/text()")[0])),
                    'cost': split_price(sanitize_data(row.xpath(".//td[@data-col-seq='4']/text()")[0])),
                    'coupon': split_price(sanitize_data(row.xpath(".//td[@data-col-seq='5']/text()")[0])),
                    'rate': float(sanitize_data(row.xpath(".//td[@data-col-seq='6']/text()")[0][:-1])),
                    'profitability': float(row.xpath(".//td[@data-col-seq='7']/text()")[0][:-1].replace(',', ''))
                }
                )

            except IndexError as e:
                print(f"Ошибка при обработке столбца.")
                print(f"Ошибка: {e}")
        # Возвращаем данные с новой ссылкой на следующую страницу и флагом для пагинации
        return data, next_page[0], tabindex
    else:
        return [], None, False

# Функция для очистки строки от ненужных символов
def sanitize_data(string):
    # Убираем все символы, кроме ASCII и символа евро
    return ''.join(c for c in string if ord(c) < 128 or ord(c) == 8364)

# Функция для разделения цены на номинал и число
def split_price(text):
    # Используем регулярное выражение для разделения строки на символ и число
    match = re.match(r"([^\d]+)([\d,]+)", text)
    result = [match.group(1), float(match.group(2).replace(",", ""))]
    return result

# Функция для записи данных в CSV файл
def write_data_to_csv(data):
    with open('data.csv', mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=['name', 'nominal', 'price', 'nkd', 'cost', 'coupon', 'rate',
                                                  'profitability'])
        # Записываем заголовок
        writer.writeheader()
        # Записываем данные
        for item in all_data:
            # Разворачиваем списки для 'nominal', 'nkd', 'cost' и 'coupon' в строковые значения
            item['nominal'] = f"{item['nominal'][0]}:{item['nominal'][1]}"
            item['nkd'] = f"{item['nkd'][0]}:{item['nkd'][1]}"
            item['cost'] = f"{item['cost'][0]}:{item['cost'][1]}"
            item['coupon'] = f"{item['coupon'][0]}:{item['coupon'][1]}"
            writer.writerow(item)


if __name__ == '__main__':
    tabindex = False
    link = base_link
    next_href = "/bonds?hl=en&page=64"
    all_data = []
    # Цикл для сбора данных до тех пор, пока не достигнута последняя страница
    while not tabindex:
        link = base_link + next_href
        print(f"Scraping page {link}...")
        # Сбор данных с текущей страницы
        data, next_href, tabindex = scrape_page_data(link)
        # Добавляем данные на текущей странице в общий список
        if not data:
            write_data_to_csv(all_data)
            print("Ошибка загрузки данных")
            print("Загруженные данные записаны в файл data.csv")
            exit(1)
        else:
            all_data.extend(data)

    print("Данные записаны в файл data.csv")


