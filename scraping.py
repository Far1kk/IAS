
from bs4 import BeautifulSoup
import re
from random import uniform
from datetime import date as libdate
import pandas as pd
import time
from datetime import datetime
from config import PATH_DRIVER, PATH_COOKIE, PATH_WS_COOKIE
from parser_wb import WildBerriesParser
from driver import GetDriver

class Scraping:
    """
    Парсинг истории, частот и запросов с сайта WordStat.
    Основное применение - парсинг дат, частот, городов и т.д.
    по указанному разделителю.
    """
    def __init__(self, driver, key_word_ls: list):
        """
        :param driver: Cсылка на директорию Chrome-драйвера
        :param key_word_ls: Список слов подлежащие парсингу
        :param aiq: additional descriptive queries или добавочные описывающие запросы. Это словарь состояищий из
        наименования группы (ключ) и списка слов, которые описывют эту группу. Напр.: Wildberries: wb,
        вб, вайлдберриз, валберис и т.д.
        """
        self.driver = driver
        self.key_word_ls = key_word_ls

    def freq(self, word: str, limit=1, plus_word='', plus_buy=True) -> dict:
        """Принимает одно анализируемое слово, ставит его ключевым (key_word), выводит его частоту и частоту
        результирующих слов за прошедший месяц. Метод возвращает словарь, где первая пара- keyword:слово, следущие пары-
        слово:{текущая дата: частота}. Пример: {'key_word': 'word1', 'word1': {'30.12.23': int}...}
        limit- количество слов, включая ключевое, которые войдут в словарь."""
        try:
            daynow = libdate.today().strftime('%d.%m.%Y')
            phrase = list()
            freq = list()
            data = {'key_word': word}
            url = 'https://wordstat.yandex.ru/#!/?words=' + str(word).replace(' ', '%20') + \
                  ('%2Bкупить' if plus_buy else '') + str(plus_word)
            self.driver.get(url=url)
            time.sleep(uniform(3, 10))
            raw = self.driver.page_source
            soup = BeautifulSoup(raw, 'html.parser')
            phrase = soup.find_all('a', class_='b-link b-phrase-link__link', limit=limit)
            freq = soup.find_all('td', class_='b-word-statistics__td b-word-statistics__td-number', limit=limit)
            for o, e in zip(phrase, freq):
                data[o.get_text()] = {f'{daynow}': e.get_text().replace(' ', '')}
            print(data)
            return data
        except Exception as ex:
            print(ex)
            self.driver.close()
            self.driver.quit()
    def history(self, aiq: dict) -> dict:
        """Принимает список ключевых слов
        key_word_ls = ['kw1', 'kw2', ... , 'kwn']
        DIQ - Describing incremental queries (Описывающие добавочные запросы). Запросы, которые формируют общую картину спроса.
        DIQ = {'Group1': [phrase1, phrase2, ... , phrase3], ... , 'Groupn': [phrase1, phrase2, ... , phrase3]}
        wordData = {'word1': {DIQ1: {'30.12.23': int, '30.01.24': int, '30.02.24': int},
        DIQ2: {'30.12.23': int, '30.01.24': int, '30.02.24': int}}} """
        try:
            main_data = dict()
            for w in self.key_word_ls:
                print('АНАЛИЗИРУЕМОЕ СЛОВО: ', w)
                group_data = dict()
                for group in list(aiq.keys()):
                    print('Анализ по группе: ', group)
                    aiq_data = {}
                    for phrase in aiq[group]:
                        print('Фраза в группе: ', phrase)
                        url = f'https://wordstat.yandex.ru/#!/history?words={str(w).replace(" ", "%20")}%20{str(phrase)}'
                        self.driver.get(url=url)
                        time.sleep(uniform(4, 10))
                        raw = self.driver.page_source
                        soup = BeautifulSoup(raw, 'html.parser')
                        odd = soup.find_all('tr', class_='odd')
                        even = soup.find_all('tr', class_='even')
                        predata = dict()
                        date_pattern = r'\d{2}\.\d{2}\.\d{4}'
                        number_pattern = r'(\d+)\.(\d+)\.(\d+)'
                        for o, e in zip(odd, even):
                            soup = BeautifulSoup(str(o), 'html.parser')
                            # Находим последнюю дату
                            dates = re.findall(date_pattern, str(o))
                            last_date = dates[-1]
                            # Находим целое число, разделенное точкой
                            number_tags = soup.find_all('span', class_='b-history__number-part')
                            integer_number = ''.join(tag.text.strip() for tag in number_tags)
                            integer_number = integer_number[:-14]
                            result = [last_date, integer_number]
                            predata[result[0]] = int(result[1])

                            soup = BeautifulSoup(str(e), 'html.parser')
                            # Находим последнюю дату
                            dates = re.findall(date_pattern, str(e))
                            last_date = dates[-1]
                            # Находим целое число, разделенное точкой
                            number_tags = soup.find_all('span', class_='b-history__number-part')
                            integer_number = ''.join(tag.text.strip() for tag in number_tags)
                            integer_number = integer_number[:-14]
                            result = [last_date, integer_number]
                            predata[result[0]] = int(result[1])

                            '''
                            match = re.sub('</span><span class="b-history__number-part">', '', str(o))
                            mas = re.findall(r"\d\d[0-9.,-]+", str(match))
                            predata[mas[1]] = mas[2] #mas[1] - дата, mas[2] - частота
                            match = re.sub('</span><span class="b-history__number-part">', '', str(e))
                            mas = re.findall(r"\d\d[0-9.,-]+", str(match))
                            predata[mas[1]] = mas[2]'''
                        aiq_data[phrase] = predata
                        print('Итог по фразе ', aiq_data[phrase])
                    '''diq_data = {'ph1': {'date1': 123, 'date2': 222}, 'ph2': {'date1': 224, 'date2': 512}}'''
                    result = {}
                    for key, value in aiq_data.items():
                        for nested_key, nested_value in value.items():
                            if nested_key in result:
                                result[nested_key] += nested_value
                            else:
                                result[nested_key] = nested_value
                    '''main_data = {w1: {group1: {date1: 123, date2: 123}, group2: {date1: 123, date2: 123}}}'''
                    group_data[str(group)] = result
                    print('Итог по группе: ', group_data)
                main_data[str(w)] = group_data
                print('Итог по слову: ', main_data)
            return main_data
        except Exception as ex:
            print(ex)
            self.driver.close()
            self.driver.quit()


    def regions(self, DIQ: dict) -> dict:
        """Принимает список ключевых слов
        key_word_ls = ['kw1', 'kw2', ... , 'kwn']
        DIQ - Describing incremental queries (Описывающие добавочные запросы). Запросы, которые формируют общую картину спроса.
        DIQ = {'Group1': [phrase1, phrase2, ... , phrase3], ... , 'Groupn': [phrase1, phrase2, ... , phrase3]}
        main_data = {'word1': {DIQ1: {'Москва': int, 'Санкт-Петербург': int},
        DIQ2: {'Москва': int, 'Санкт-Петербург': int}}} """
        try:
            main_data = dict()
            for w in self.key_word_ls:
                print('АНАЛИЗИРУЕМОЕ СЛОВО: ', w)
                group_data = dict()
                for group in list(DIQ.keys()):
                    print('Анализ по группе: ', group)
                    diq_data = {}
                    for phrase in DIQ[group]:
                        print('Фраза в группе: ', phrase)
                        url = f'https://wordstat.yandex.ru/#!/regions?filter=cities&words={str(w).replace(" ", "%20")}%20{str(phrase)}'
                        self.driver.get(url=url)
                        time.sleep(uniform(4, 10))
                        raw = self.driver.page_source
                        soup = BeautifulSoup(raw, 'html.parser')
                        odd = soup.find_all('td', class_='b-regions-statistic__td b-regions-statistic__td_type_cities')
                        even = soup.find_all('td', class_='b-regions-statistic__td b-regions-statistic__td_type_number')
                        freq = even[::2]
                        predata = dict()
                        k = 0
                        for o, e in zip(odd, freq):
                            soup = BeautifulSoup(str(o), 'html.parser')
                            # Находим последнюю дату
                            city = soup.get_text()
                            soup = BeautifulSoup(str(e), 'html.parser')
                            popul = soup.get_text().replace(' ', '')
                            if k == 0:
                                freq_most_pop = popul
                                k += 1
                            result = [city, popul]
                            if (int(result[1]) >= int(freq_most_pop)/15) and (int(result[1]) >= 100):
                                predata[result[0]] = int(result[1])
                            else:
                                pass

                        def remove_duplicates(dictionary):
                            keys = list(dictionary.keys())
                            for i in range(len(keys)):
                                for j in range(i + 1, len(keys)):
                                    if keys[i] in keys[j] or keys[j] in keys[i]:
                                        del dictionary[keys[j]]
                                        break
                            return dictionary
                        predata = remove_duplicates(predata)
                        diq_data[phrase] = predata
                        print('Итог по фразе ', diq_data[phrase])
                    '''diq_data = {'ph1': {'date1': 123, 'date2': 222}, 'ph2': {'date1': 224, 'date2': 512}}'''
                    result = {}
                    for key, value in diq_data.items():
                        for nested_key, nested_value in value.items():
                            if nested_key in result:
                                result[nested_key] += nested_value
                            else:
                                result[nested_key] = nested_value
                    '''main_data = {w1: {group1: {date1: 123, date2: 123}, group2: {date1: 123, date2: 123}}}'''
                    group_data[str(group)] = result
                    print('Итог по группе: ', group_data)
                main_data[str(w)] = group_data
                print('Итог по слову: ', main_data)
        except Exception as ex:
            print(ex)
        else:
            return main_data

    def uploadToExcel(self, mas, path, sheet_name):
        '''mas = {'word1': {'wb':{'date1': 123, 'date2': 241}, 'simple':{'date1': 243, 'date2': 543}},
        'word2': {'wb':{'date1': 5324, 'date2': 4664}, 'simple':{'date1': 4231, 'date2': 2435}}}'''
        data = []
        # Создаем список заголовков столбцов
        columns = ['Запрос', 'Площадка']
        # Получаем список всех уникальных ключей date из вложенных словарей
        dates = set()
        for value in mas.values():
            for nested_value in value.values():
                dates.update(nested_value.keys())
        # Сортируем список дат по алфавиту
        dates = list(dates)
        dates.sort(key=lambda date: datetime.strptime(date, '%d.%m.%Y'))
        # Добавляем заголовки столбцов с датами в список заголовков
        columns.extend(dates)
        # Создаем список данных для записи в Excel
        try:
            for key, value in mas.items():
                for nested_key, nested_value in value.items():
                    row = [key, nested_key]
                    ''' for key, value in nested_value.items():
                        datetime.strptime(str(key), '%d.%m.%Y')
                        print(str(key))'''
                    row.extend(nested_value[date] for date in dates)
                    data.append(row)
        except Exception as ex:
            print(ex)
        # Создаем DataFrame на основе списка данных и заголовков
        df = pd.DataFrame(data, columns=columns)
        # Записываем DataFrame в Excel
        df.to_excel(path, sheet_name=sheet_name, index=False)

    def uploadToExcelRegions(self, mas, path, sheet_name):
        '''mas = {'word1': {'wb':{'date1': 123, 'date2': 241}, 'simple':{'date1': 243, 'date2': 543}},
                'word2': {'wb':{'date1': 5324, 'date2': 4664}, 'simple':{'date1': 4231, 'date2': 2435}}}'''
        columns = ['Запрос', 'Площадка']
        data = []
        # Получаем список всех уникальных ключей date из вложенных словарей
        df_list = []
        try:
            for key, value in mas.items():
                for nested_key, nested_value in value.items():
                    cities = list(nested_value.keys())
                    columns.extend(cities)
                    row = [key, nested_key]
                    row.extend(nested_value[city] for city in cities)
                    data.append(row)
                    df_list.append(pd.DataFrame(data, columns=columns))
                    data = []
                    columns = ['Запрос', 'Площадка']
        except Exception as ex:
            print(ex)
        k = 0
        startrow = 0
        for df in df_list:
            if k == 0:
                k += 1
                with pd.ExcelWriter(path, mode='w', engine="openpyxl") as writer:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    startrow += df.shape[0] + 1
            else:
                with pd.ExcelWriter(path, mode='a', engine="openpyxl", if_sheet_exists = 'overlay') as writer:
                    df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=startrow)
                    startrow += df.shape[0] + 1

    def uploadToExcel1(self, mas, path, sheet_name):
        """Загружает в эксель пачку пропарсеных слов(список из словарей ключевых слов),
        где индексы-слова, колонки-даты.
        ВНИМАНИЕ! данная функция не создает файл excel, необходимо предварительно создать файл!
        Справка: Пачка - список из словарей разных ключевых слов. Популяция - словарь состоящий из обязательно одного
        ключевого слова, и других слов, относящихся к этому ключу. Особь - словарь из слова и его историей частот.
        {'word1': {'wb':{'date1': 123, 'date2': 241}, 'simple':{'date1': 243, 'date2': 543}}, 'word2': {'wb':{'date1': 5324, 'date2': 4664}, 'simple':{'date1': 4231, 'date2': 2435}}}"""
        """{'key_word': 'word1', 'word1': {'30.10.23': int, '30.11.23': int,
                                                '30.12.23': int}, 'word2': {'30.10.23': int, '30.11.23': int, '30.12.23': int}"""
        words = list()
        months = list()
        l = 0
        # Выбираем все не ключевые слова из популяций пачки
        for p in range(0, len(mas)):
            for word in list(mas[p].keys())[1:]:
                words.append(word)
                l += 1
                if l <= 1:
                    months = list(mas[p][word].keys())
        WORDSDATA = pd.DataFrame([], index=words, columns=months)
        WORDSDATA.index.name = 'Word'
        print(words)
        print(months)
        for num_pop in range(0, len(mas)):
            for word in list(mas[num_pop].keys())[1:]:
                for date in list(mas[num_pop][word].keys()):
                    try:
                        WORDSDATA.loc[word, date] = mas[num_pop][word][date]
                    except Exception as ex:
                        print('ошибка' + str(ex))
        try:
            with pd.ExcelWriter(str(path), engine='openpyxl', mode='a', if_sheet_exists='overlay') as wrt:
                startrow = wrt.sheets[str(sheet_name)].max_row
                WORDSDATA.to_excel(wrt, sheet_name=str(sheet_name), startrow=startrow, header=False)
        except Exception as ex:
            print('Создание таблицы. ' + str(ex))
            with pd.ExcelWriter(str(path), engine='openpyxl', mode='a', if_sheet_exists='overlay') as wrt:
                WORDSDATA.to_excel(wrt, sheet_name=str(sheet_name), header=True)
        print('Загрузка данных в эксель успешно выполнена')


if __name__ == '__main__':
    def parsing(path_read, sheet_read, path_write, sheet_write, changeUser=10, uploadcount=6, limitparsing=19, limitpop=1):
        # Создаем образ драйвера, указываем директорию драйвера и куки
        AIQ = {'simple': [''], 'wb': ['+вб', '+валбериз', '+вайлдберриз'], 'ozon': ['+озон', '+ozon']}
        authdriver = GetDriver(PATH_DRIVER, PATH_WS_COOKIE)
        authdriver = authdriver.driver()
        pars = pars2 = pars3 = 0
        mas = list()
        words_list = pd.read_excel(path_read, sheet_name=sheet_read)
        words_list = words_list['Наименование']
        data_word = Scraping(authdriver, words_list)
        #main_data = data_word.history(DIQ=DIQ)
        main_data = data_word.history(AIQ)
        data_word.uploadToExcel(main_data, path=path_write, sheet_name=sheet_write)
        '''for i in words_list:
            data_of_word = Scraping(authdriver, words_list)
            #wordata = data_of_word.freq(i, limit=limitpop, plus_word='%2Bвалберис', plus_buy=False)
            main_data = data_of_word.history(DIQ=DIQ)
            pars += 1
            pars2 += 1
            pars3 += 1
            if pars >= changeUser:
                print('Отключение драйвера...')
                authdriver.close()
                authdriver.quit()
                authdriver = GetDriver(PATH_DRIVER, PATH_COOKIE)
                authdriver = authdriver.driver()
                pars = 0
            elif pars2 >= uploadcount:
                data.uploadToExcel(mas, path=path_write, sheet_name=sheet_write)
                mas = list()
                pars2 = 0
            elif pars3 >= limitparsing:
                mas = list()
                authdriver.close()
                authdriver.quit()
                break
        data.uploadToExcel(mas, path=path_write, sheet_name=sheet_write)'''
    def words_to_list():
        app = WildBerriesParser(PATH_COOKIE)
        word_data = app.get_catalogue_data()
        df = pd.DataFrame({'Наименование': word_data})
        df.to_excel('./data/category_wb.xlsx', index=False)

    #words_to_list()

    parsing(path_read='data/category_wb.xlsx', sheet_read='filter', path_write='data/Категории2.xlsx', sheet_write='Частотность')