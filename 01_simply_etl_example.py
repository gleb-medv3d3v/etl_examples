import sqlite3
import requests
import pandas as pd

CONN = sqlite3.connect('sqlite3.db')


# скачивание данных с гитхаба
def extract_data(url, date):
    # format(day=x) - для подстановки даты из словаря dates_list в ссылку на .csv 
    exchange_data = [pd.read_csv(url.format(day=x)) for x in date]
    result_df = pd.concat(exchange_data, ignore_index=True)
    return result_df


# слияние двух датафреймов
def merge_data(data1, data2):
  # удаление дублирующего столбца 'currency_from' из первого дф и слияние со вторым + переименование столбцов
  merged_dfs = data1.drop(labels=['currency_from'], axis=1).merge(data2).rename(columns={'currency_to': 'code', 'currency': 'base', 'amount': 'rate'})
  # заглавные буквы в значениях столбца 'code'
  merged_dfs['code'] = merged_dfs['code'].str.upper()
  return merged_dfs
  

# вставка данных в БД  
def insert_to_db(data, table_name, conn):
    data.to_sql(table_name, con=conn, if_exists='replace', index=False)  

# -------------------------------------------------------------------------

dates_list = ['2021-01-01', '2021-01-02', '2021-01-03', '2021-01-04']

def main(date, conn):

  currency = extract_data('https://raw.githubusercontent.com/datanlnja/airflow_course/main/excangerate/{day}.csv', dates_list)
  data = extract_data('https://raw.githubusercontent.com/datanlnja/airflow_course/main/data/{day}.csv', dates_list)

  mg_data = merge_data(currency, data)

  insert_to_db(mg_data, 'data', CONN)

# запись данных построчно из готового дф в таблицу бд sqlite3
for date in dates_list:
  main(date, conn)


conn = sqlite3.connect('sqlite3.db')
cursor = conn.cursor()
cursor.execute("SELECT SUM(rate*value) FROM data")

rows = cursor.fetchall()

for row in rows:
    print(row)
    
conn.close()