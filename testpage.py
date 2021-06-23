from DataFormatCorrection import *
import datetime
from psycopg2 import Error
import psycopg2

def updateloghourse(datafromserver):
    upgradedata = datetime.datetime.strptime(datafromserver, "%Y-%m-%d %H:%M:%S")
    upgradedata = upgradedata + datetime.timedelta(hours=3)
    return upgradedata
def updateloghourse(datafromserver):
    upgradedata = datetime.datetime.strptime(datafromserver, "%Y-%m-%d %H:%M:%S")
    upgradedata = upgradedata + datetime.timedelta(hours=3)
    return upgradedata


def realdatatame():
    t = datetime.datetime.date(datetime.datetime.now())
    # t = t + datetime.timedelta(hours=48) #Увеличение\уменьшение времени
    t = t.strftime('%d_%m_%Y')
    return t

m = '2021-06-23 23:53:22'
cryptomoney = 'BTC_ETH'
b_data = '2021-06-21'
name_table = f'tradehistory_{cryptomoney}_{realdatatame()}'
h = updateloghourse(m)
print(type(h))

def tes_cleaner(name_table,b_data):
    try:
        # Подключение к существующей базе данных
        connection = psycopg2.connect(user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password="111111",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="postgres")
        cursor = connection.cursor()
        # Выполнение SQL-запроса для вставки данных в таблицу
        insert_query = f"""DELETE FROM {name_table} WHERE date < '{b_data}'"""

        # Провести удаление.
        cursor.execute(insert_query)
        connection.commit()
        print("Таблица подготовлена")
        # lastdatetimetrade = check_last_date_edit_table(name_table)
        print('Метка 4')
        # return lastdatetimetrade


    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()

# tes_cleaner(name_table,h)


f_data = datetime.date(h.year, h.month, h.day)
print(f_data)