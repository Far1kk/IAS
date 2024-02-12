import random
import pandas as pd
import requests
import time
import json
from datetime import date
from os import path

class WildBerriesParser():

    def __init__(self):
        """
        :param headers: Параметры заголовка для запросов requests
        :param run_date: Сегодняшняя дата для обозначений файлов Excel и атрибута времени в БД
        :param product_cards: Информация спарсенных товаров
        :param directory: Директория для сохранения xlsx-файлов
        :param region: Словарь наименований регионов их коды на WB (параметр dest в ссылках)
        """
        self.headers = {'Accept': "*/*",
                        'User-Agent': "Chrome/51.0.2704.103 Safari/537.36"}
        self.run_date = date.today()
        self.product_cards = []
        self.directory = path.dirname(__file__) + r'\data'
        self.region = {'армянск': 123585600, 'москва': -434560}
        self.result_path = str()
        self.user_region = str()
        self.user_query = dict()

    def download_current_catalogue(self) -> str:
        """
        Загрузка JSON-файла каталога на WB
        :return: Путь файла
        """
        local_catalogue_path = path.join(self.directory, 'wb_catalogue.json')

        if (not path.exists(local_catalogue_path)
                or date.fromtimestamp(int(path.getmtime(local_catalogue_path)))
                > self.run_date):
            url = ('https://static-basket-01.wb.ru/vol0/data/'
                   'main-menu-ru-ru-v2.json')
            response = requests.get(url, headers=self.headers).json()

            with open(local_catalogue_path, 'w', encoding='UTF-8') as my_file:
                json.dump(response, my_file, indent=2, ensure_ascii=False)

        return local_catalogue_path

    def traverse_json(self, parent_category: list, flattened_catalogue: list):
        """
        Вывод всех вложенных параметров категорий
        """
        for category in parent_category:

            try:
                flattened_catalogue.append({
                    'name': category['name'],
                    'url': category['url'],
                    'shard': category['shard'],
                    'query': category['query']
                })
            except KeyError:
                continue

            if 'childs' in category:
                self.traverse_json(category['childs'], flattened_catalogue)

    def process_catalogue(self, local_catalogue_path: str) -> list:
        """
        Форматирование JSON-файла каталога
        :return: словарь
        """
        catalogue = []
        with open(local_catalogue_path, 'r', encoding='UTF-8') as my_file:
            self.traverse_json(json.load(my_file), catalogue)
        return catalogue

    def extract_category_data(self, catalogue: list, user_input: str) -> tuple:
        """
        Поиск категории, введеной пользователем
        :return: параметры категории
        """
        for category in catalogue:

            if (user_input.split("https://www.wildberries.ru")[-1]
                    == category['url'] or user_input == category['name']):
                self.user_query = {'search_url': category['url'], 'name': category['name'], 'category': True}
                print(f'Название категории: {self.user_query}')
                return category['name'], category['shard'], category['query'], category['url']

    def get_products_on_page(self, page_data: dict) -> list:
        """
        Структурирует информацию по всем товарам из полученного списка всех товаров страницы
        :return: Список товаров с параметрами
        """
        products_on_page = []

        for item in page_data['data']['products']:
            print(item)
            cpm_data = lambda y: item['log'][str(y)] if len(item['log']) > 0 else 0  # Проверка на наличие данных
            products_on_page.append({
                'url': f"https://www.wildberries.ru/catalog/"
                          f"{item['id']}/detail.aspx",
                'article': item['id'],
                'title': item['name'],
                'seller_id': item['supplierId'],
                'seller_name': item['supplier'],
                'brand_name': item['brand'],
                'brand_id': item['brandId'],
                'old_price': int(item['priceU'] / 100),
                'price': int(item['salePriceU'] / 100),
                'rating': item['reviewRating'],
                'reviews': item['feedbacks'],
                'pictures': item['pics'],
                'position': cpm_data('promoPosition'),
                'cpm': cpm_data('cpm')})

        return products_on_page

    def add_data_from_page(self, url: str):
        """
        Загрузка всех товаров со страницы WB
        """
        try:
            response = requests.get(url, headers=self.headers).json()
            page_data = self.get_products_on_page(response)

            if len(page_data) > 0:
                self.product_cards.extend(page_data)
                print(f"Добавлено товаров: {len(page_data)}")
            else:
                print('Загрузка товаров завершена')
                return True
        except json.decoder.JSONDecodeError:
            print('File is empty')

    def get_all_products_in_category(self, category_data: tuple, dest: int):
        """
        Создание ссылок категорий
        """
        for page in range(1, 4):  # Количество анализируемых страниц (предусматривается аналитика по топ 200 продавцам или 2 страницы)
            if page % 30 == 0:
                time.sleep(5)  # После 30 страниц WB начинает хуже выдавать информацию
            print(f"Загружаю товары со страницы {page}")
            url = (f"https://catalog.wb.ru/catalog/{category_data[1]}/"
                   f"catalog?appType=1&{category_data[2]}&curr=rub"
                   f"&dest={dest}&page={page}&sort=popular&spp=24")
            if self.add_data_from_page(url):
                break

    def extract_models(self, data: dict) -> list:

        products = data['data']['products']
        models = []

        for product in products:
            sizes = product['sizes']

            for size in sizes:
                stocks = size['stocks']

                if not len(stocks) == 0:
                    selected_stock = size['wh']
                    ftime = size['time1'] + size['time2']
                    qty = None
                    priority = None

                    for stock in stocks:

                        if selected_stock == stock['wh']:
                            qty = stock['qty']
                            priority = stock['priority']
                            break

                    model = {
                        'origName': size['origName'],
                        'wh': selected_stock,
                        'qty': qty,
                        'priority': priority,
                        'ftime': ftime
                    }

                    models.append(model)

                else:
                    pass

        return models

    def get_sales_data(self, dest):

        for card in self.product_cards:
            try:
                # По этой ссылке можно узнать количество (qty) на каждом складе (wh) каждого размера (size) у продавца
                # В качестве результата выдается ближайший склад по времени time2.
                url_wh = f"https://card.wb.ru/cards/v1/detail?appType=1&curr=rub&dest={dest}&spp=29&nm={card['article']}"
                response = requests.get(url_wh, headers=self.headers).json()
                print(response)
                models = self.extract_models(response)
                print(models)
                card['stocks'] = models

                sum_qty = 0
                sum_ftime = 0
                count = 0

                for model in models:
                    if model['ftime'] is not None:
                        sum_qty += model['qty']
                        sum_ftime += model['ftime']
                        count += 1

                avg_qty = sum_qty / count
                avg_ftime = sum_ftime / count

                card['avg_delivery'] = avg_ftime
                card['avg_units'] = avg_qty
                card['units_in_stocks'] = sum_qty

                url = (f"https://product-order-qnt.wildberries.ru/by-nm/"
                       f"?nm={card['article']}")
                response = requests.get(url, headers=self.headers).json()
                if len(response) <= 0:
                    card['sales'] = 'нет данных'
                else:
                    card['sales'] = response[0]['qnt']
                print('Данные карточки:' + str(response))
                # print('Данные продаж:' + str(response[0]['qnt']))
            except requests.ConnectTimeout:
                card['sales'] = 'нет данных'
            print(f"Собрано карточек: {self.product_cards.index(card) + 1}"
                  f" из {len(self.product_cards)}")

    def save_to_excel(self, file_name: str, region: str) -> str:

        data = pd.DataFrame(self.product_cards)
        self.result_path = (f"{path.join(self.directory, file_name)}_"
                            f"{self.run_date.strftime('%Y-%m-%d')}_{region}.xlsx")
        writer = pd.ExcelWriter(self.result_path)
        data.to_excel(writer, 'data', index=False)
        writer.close()
        return self.result_path

    def get_all_products_in_search_result(self, key_word: str, dest):

        for page in range(1, 4):
            print(f"Загружаю товары со страницы {page}")
            if page == 1:
                url = (f"https://search.wb.ru/exactmatch/ru/common/v4/search?"
                       f"TestGroup=control&TestID={random.randint(1, 500)}&appType=1&curr=rub&"
                       f"dest={dest}&query={'%20'.join(key_word.split())}&resultset=catalog&"
                       f"sort=popular&spp=27&suppressSpellcheck=false&uclusters=0")
            else:
                url = (f"https://search.wb.ru/exactmatch/ru/common/v4/search?"
                       f"TestGroup=control&TestID={random.randint(1, 500)}&appType=1&curr=rub&"
                       f"dest={dest}&page={page}&query={'%20'.join(key_word.split())}&resultset=catalog&"
                       f"sort=popular&spp=27&suppressSpellcheck=false&uclusters=0")
            time.sleep(5)
            if self.add_data_from_page(url):
                break

    def get_names(self, dictionary):
        names = list()
        for d in dictionary:
            if isinstance(d, dict):
                for key, value in d.items():
                    if key == 'name':
                        names.append(value)
                    elif isinstance(value, dict):
                        self.get_names(value)
                    elif isinstance(value, list):
                        for item in value:
                            self.get_names(item)
        return names

    def run_parser(self):
        self.user_region = str(input("Введите регион: "))
        try:
            region = self.region[self.user_region.lower().strip()]
        except Exception:
            print('Региона нет в списке')
            return

        instructons = """Введите 1 для парсинга категории целиком,
        2 — по ключевым словам: """
        mode = input(instructons)
        if mode == '1':
            local_catalogue_path = self.download_current_catalogue()
            print(f"Каталог сохранен: {local_catalogue_path}")
            processed_catalogue = self.process_catalogue(local_catalogue_path)
            input_category = input("Введите название категории или ссылку: ")
            category_data = self.extract_category_data(processed_catalogue,
                                                       input_category)
            if category_data is None:
                print("Категория не найдена")
            else:
                print(f"Найдена категория: {category_data[0]}")
            self.get_all_products_in_category(category_data, region)
            self.get_sales_data(region)
            print(f"Данные сохранены в {self.save_to_excel(category_data[0], self.user_region)}")

        if mode == '2':
            key_word = input("Введите запрос для поиска: ")
            self.user_query = {'search_url': f'https://www.wildberries.ru/catalog/0/search.aspx?search={key_word}',
                               'name': str(key_word), 'category': False}
            self.get_all_products_in_search_result(key_word, region)
            self.get_sales_data(region)
            print(f"Данные сохранены в {self.save_to_excel(key_word, self.user_region)}")
    def get_catalogue_data(self):
        local_catalogue_path = r'C:\Users\bogdan\parser_wordstat\wb_catalogue.json'
        processed_catalogue = self.process_catalogue(local_catalogue_path)
        category_data = self.get_names(processed_catalogue)
        return category_data

