import numpy as np
import pandas as pd
import os
from scipy.stats import pearsonr
from scipy.stats import spearmanr
from datetime import date, datetime
from os import path
import ast

class processing:
    
    def __init__(self, path_xlsx: str, date_str: str, region: str, query: dict):
        self.region = region
        self.date = date_str #datetime.strptime(date_str, '%Y-%m-%d').date() #Для отладки
        self.query = query
        self.path = path_xlsx
        self.chdir = os.chdir("C:/Users/bogdan/parser_wordstat")
        self.dataset = pd.read_excel(path_xlsx)
        self.MARKET = pd.DataFrame()
        self.MARKET_STATS = pd.DataFrame()
        self.run_date = date.today()
        self.directory = path.dirname(__file__)
        self.date_db = pd.DataFrame()
        self.product_db = pd.DataFrame()
        self.region_db = pd.DataFrame()
        self.query_db = pd.DataFrame()
        self.fact_product_db = pd.DataFrame()
        self.fact_stock_db = pd.DataFrame()
        self.stock_db = pd.DataFrame()
        self.fact_region_db = pd.DataFrame()
        self.norm_stock = pd.DataFrame()
    
    def recoding_data(self):
        #new_data = new_data.drop(['Ссылка', 'article', 'brand_id'], axis=1)
        self.dataset['sales'].replace({'нет данных': 0}, inplace=True)
        self.dataset['position']= self.dataset.index + 1
        self.dataset['discount'] = 100-(self.dataset['old_price']*100)/self.dataset['price']
        self.dataset = self.dataset.astype({'title':'category', 'seller_name':'category', 
                                    'url': 'category', 'article': np.int64,
                                    'brand_id': np.int64, 'seller_id': np.int64, 
                                    'brand_name':'category', 'old_price':np.float64, 
                                    'price':np.int64, 'rating':np.float64,
                                    'reviews':np.float64, 'pictures':np.float64,
                                    'position':np.float64, 'cpm':np.float64, 
                                    'avg_delivery':np.float64, 'avg_units':np.float64,
                                    'units_in_stocks':np.int64,
                                    'sales':np.int64, 'discount': np.float64})
        return self.dataset
    
    def aWordstat(self, sheet_name):
        WORDS = pd.read_excel(self.path, sheet_name=sheet_name)
        WO = WORDS.copy()
        # recode the data
        for i in WO.columns:
            print(i)
            if i == 'Word':
                WO = WO.astype({'Word':'category'})
            else:
                WO = WO.astype({f'{i}':np.float64})
        # make statistic dataframe 
        basic_stats = pd.DataFrame(columns=['Слово', 'Ср. частота', 'Минимум', 'Максимум', 'Ср. темп', 'Ст. откл', 'Темп роста/год'])
        # count the indicators
        for i in range(0, len(WO)):
            word = WO.loc[i][0]
            mean_trend = pd.DataFrame([(WO.loc[i][k])/(WO.loc[i][k-1])*100 for k in range(2, len(WO.loc[i]))]).mean()
            mean = WO.loc[i][1:].mean()
            min_val = WO.loc[i][1:].min()
            max_val = WO.loc[i][1:].max()
            std_dev = WO.loc[i][1:].std()
            trend_year = (WO.loc[i][13:25].sum()*100)/ (WO.loc[i][1:13].sum())
            # add the results to the dataframe
            basic_stats = basic_stats.append({'Слово': word, 'Ср. частота': mean, 
                                              'Минимум': min_val, 'Максимум': max_val, 
                                              'Ср. темп': mean_trend[0], 
                                              'Ст. откл': std_dev, 
                                              'Темп роста/год': trend_year}, 
                                             ignore_index=True)
        return basic_stats
        
    
    def dataset_proc(self):
        dup = self.dataset.duplicated(subset='article')
        self.dataset.drop(index= dup[dup==True].index, inplace=True)
        self.dataset['revenue'] = self.dataset['sales']*self.dataset['price']
        self.dataset['market_share'] = (self.dataset['revenue']/self.dataset['revenue'].sum())*100
        self.dataset.sort_values(by=['market_share'], inplace=True, ascending=False)
        
    def date_proc(self):
        
        self.date_db = pd.DataFrame({'date': self.date, 
                                'month_number': self.date.strftime('%m'), 
                                'day_of_week': self.date.strftime('%A'), 
                                'month_name': self.date.strftime('%B'),
                                'num_week': self.date.strftime('%W'), 
                                'quarter': (self.date.month - 1) // 3 + 1},
                               columns=['date', 'month_number', 'day_of_week',
                                        'month_name', 'num_week', 'quarter'], index=[0])
    
    def product_proc(self):
        
        self.product_db = pd.DataFrame({'article': self.dataset['article'], 
                                   'seller_id': self.dataset['seller_id'],
                                   'brand_id': self.dataset['brand_id'], 
                                   'brand_name': self.dataset['brand_name'],
                                   'seller_name': self.dataset['seller_name'], 
                                   'url': self.dataset['url']},
                                  columns=['article','seller_id','brand_id',
                                           'brand_name', 'seller_name', 'url'])
        
    def region_proc(self):
        
        self.region_db = pd.DataFrame({'name': self.region}, columns=['name'], index=[0])
        
    def query_proc(self):
        
        self.query_db = pd.DataFrame({'url': self.query['search_url'], 
                                      'name': self.query['name'], 
                                 'category': self.query['category']}, 
                                columns=['name', 'category', 'url'], index=[0])
        
    def fact_product_proc(self):
        
        self.fact_product_db = pd.DataFrame({'article': self.dataset['article'], 
                                        'seller_id': self.dataset['seller_id'],
                                        'date_id': self.date_db['date'][0], 
                                        'sales': self.dataset['sales'], 
                                        'price': self.dataset['price'], 
                                        'rating': self.dataset['rating'], 
                                        'reviews': self.dataset['reviews'],
                                        'old_price': self.dataset['old_price'], 
                                        'title': self.dataset['title'],
                                        'pictures': self.dataset['pictures'], 
                                        'cpm': self.dataset['cpm'],
                                        'units_in_stocks': self.dataset['units_in_stocks'], 
                                        'avg_units': self.dataset['avg_units']},
                                       columns=['article','seller_id','date_id','sales', 
                                                'price', 'rating', 'reviews', 'old_price', 
                                                'title', 'pictures', 'cpm', 
                                                'units_in_stocks', 'avg_units'])
    def normal_stock(self):
        
        prod_stock = self.dataset[['article', 'seller_id', 'stocks']]
        
        new_columns = ['origName', 'wh', 'qty', 'priority', 'ftime']
        # Create an empty list to store the normalized dictionaries
        normalized_data = []
        # Iterate over each row of the DataFrame
        for index, row in prod_stock.iterrows():
            # Normalize the dictionaries in the 'склады' column
            stocks_list = ast.literal_eval(row['stocks'])
            normalized_dicts = pd.json_normalize(stocks_list)
            # Add the 'артикул' and 'номер продавца' columns to the normalized dictionaries
            normalized_dicts['article'] = row['article']
            normalized_dicts['seller_id'] = row['seller_id']
            
            # Append the normalized dictionaries to the list
            normalized_data.append(normalized_dicts)
        
        # Concatenate the list of normalized dictionaries into a new DataFrame
        norm_stock = pd.concat(normalized_data, ignore_index=True)
        # Reorder the columns of the new DataFrame
        norm_stock = norm_stock[['article', 'seller_id'] + new_columns]
        rename_columns = {'wh':'stock_id', 'ftime': 'delivery_time', 'origName': 'model_name', 'qty':'quantity'}
        self.norm_stock = norm_stock.rename(columns=rename_columns)
        self.norm_stock = self.norm_stock.reset_index()
        self.norm_stock = self.norm_stock.drop(self.norm_stock[self.norm_stock['quantity'] <= 7].index)
    
    def fact_stock_proc(self):
        
        self.fact_stock_db = pd.DataFrame({'article': self.norm_stock['article'], 
                                      'seller_id': self.norm_stock['seller_id'],
                                      'date_id': self.date_db['date'][0], 
                                      'stock_id': self.norm_stock['stock_id'], 
                                      'model_name': self.norm_stock['model_name'], 
                                      'quantity': self.norm_stock['quantity'], 
                                      'delivery_time': self.norm_stock['delivery_time']},
                                     columns=['article','seller_id','date_id','stock_id', 
                                              'model_name', 'quantity', 'delivery_time'])
    def stock_proc(self):
        
        self.stock_db = pd.DataFrame({'stock_id': self.norm_stock['stock_id'].unique()},
                                columns=['stock_id'])
        
    def fact_region_proc(self):
        
        self.fact_region_db = pd.DataFrame({'article': self.dataset['article'], 
                                       'seller_id': self.dataset['seller_id'],
                                       'date_id': self.date_db['date'][0],
                                       'region_id': self.region_db['name'][0],
                                       'query_id': self.query_db['url'][0],
                                       'position': self.dataset['position'],
                                       'avg_delivery': self.dataset['avg_delivery'],
                                       'market_share': self.dataset['market_share']},
                                      columns=['article','seller_id','date_id',
                                               'region_id', 'query_id', 'position', 
                                               'avg_delivery', 'market_share'])
    
    def save_to_csv(self, file_name):
        
        full_path = f"{path.join(self.directory, file_name)}"
        # Сохраняем каждый датафрейм на отдельном листе в файле
        self.date_db.to_csv(full_path + '_date_db.csv', index=False, encoding='cp1251')
        self.product_db.to_csv(full_path + '_product_db.csv', index=False, encoding='cp1251')
        self.region_db.to_csv(full_path + '_region_db.csv', index=False, encoding='cp1251')
        self.query_db.to_csv(full_path + '_query_db.csv', index=False, encoding='cp1251')
        self.fact_product_db.to_csv(full_path + '_fact_product_db.csv', index=False, encoding='cp1251')
        self.fact_stock_db.to_csv(full_path + '_fuct_stock_db.csv', index=False, encoding='cp1251')
        self.stock_db.to_csv(full_path + '_stock_db.csv', index=False, encoding='cp1251')
        self.fact_region_db.to_csv(full_path + '_fact_region_db.csv', index=False, encoding='cp1251')
        print('Данные csv сохранены в ', self.directory + r'\db')
        
    def save_to_excel(self, file_name: str) -> str:
        
        result_path = (f"{path.join(self.directory, file_name)}_proc.xlsx")
        writer = pd.ExcelWriter(result_path)
        self.dataset.to_excel(writer, 'dataset', index=False)
        writer.close()
        """with pd.ExcelWriter(result_path, engine='openpyxl', mode='a', 
                            if_sheet_exists='overlay') as wrt:
            self.MARKET.to_excel(wrt, sheet_name='Рынок')
            dr = self.MARKET.shape[0] + 2
            self.MARKET_STATS.to_excel(wrt, sheet_name='Рынок', startrow=dr)"""
        return result_path
    
    def path_result(self) -> str:
        
        return f'{self.path.replace(".xlsx", "")}'
        
    def run_analytic(self):
        self.recoding_data()
        self.dataset_proc()
        self.normal_stock()
        self.date_proc()
        self.product_proc()
        self.region_proc()
        self.query_proc()
        self.fact_product_proc()
        self.fact_stock_proc()
        self.stock_proc()
        self.fact_region_proc()
       # self.save_to_csv(self.path_result())
        print(f'Файлы сохранены в {self.save_to_excel(self.path_result())}')
        return self.dataset
    
    def pearson_spearman(self):
        
        dataset_F = self.dataset.select_dtypes('float')
        # Pearson
        C_P = pd.DataFrame([], index=dataset_F.columns, columns=dataset_F.columns) 
        # p-value
        P_P = pd.DataFrame([], index=dataset_F.columns, columns=dataset_F.columns)
        # Spearman
        C_S = pd.DataFrame([], index=dataset_F.columns, columns=dataset_F.columns)
        # p-value
        P_S = pd.DataFrame([], index=dataset_F.columns, columns=dataset_F.columns)
        for x in dataset_F.columns:
            for y in dataset_F.columns:
                C_P.loc[x,y], P_P.loc[x,y] = pearsonr(dataset_F[x], dataset_F[y])
                C_S.loc[x,y], P_S.loc[x,y] = spearmanr(dataset_F[x], dataset_F[y])
        return C_P, C_S, P_P, P_S