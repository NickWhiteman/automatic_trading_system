import psycopg2
import datetime
import time
from psycopg2 import Error
from DataFormatCorrection.UpdateData import *
from threading import Thread
from time import sleep



# def ga1():
#     for i in range(99):
#         print(i)
#         sleep(0.5)
#
#
# th1 = Thread(target=ga1, daemon=True)
# th1.start()
#
# def ga2():
#     for i in range(200,700):
#         print(i)
#         sleep(1)
# ga2()

#Преобразование str в datetime.datetime
# def update_str_for_datatime2(datafromserver):
#     upgradedata = datetime.datetime.strptime(datafromserver, "%Y-%m-%d %H:%M:%S")
#     return upgradedata
#
# data1 = update_str_for_datatime('2021-06-10 23:02:00')
# data2 = update_str_for_datatime('2021-06-12 00:22:07')
#
# d_truncated1 = datetime.date(data1.year, data1.month, data1.day)
# d_truncated2 = datetime.date(data2.year, data2.month, data2.day)
#
# print(d_truncated2)
# print('--------------------')
# resz = d_truncated1-d_truncated2
# print(resz)
# resz = str(resz)
# resz = resz.split()[0]
# print(type(resz))
#
#
# if resz <= '-1':
#     print('разница более суток')
#     pass
#
# else:
#     print('Похуй')

# aa = datetime.date(data1.year, data1.month, data1.day)
# bb = datetime.date(data2.year, data2.month, data2.day)
# cc = aa-bb
# print(cc) # output days and time
# dd = str(cc)
# print(dd.split()[0])

# name_table = f'tradehistory_btc_eth_12_06_2021_2'
# newdataD = str(d_truncated2)
# print(newdataD)
#
# def chectablefromDB11(name_table,newdataD):
#     try:
#         # Подключение к существующей базе данных
#         connection = psycopg2.connect(user="postgres",
#                                       # пароль, который указали при установке PostgreSQL
#                                       password="111111",
#                                       host="127.0.0.1",
#                                       port="5432",
#                                       database="postgres")
#         cursor = connection.cursor()
#         # Выполнение SQL-запроса для вставки данных в таблицу
#         insert_query = f"""DELETE FROM {name_table} WHERE date < '{newdataD}'"""
#
#         # Получить результат
#         cursor.execute(insert_query)
#         connection.commit()
#
#
#     except (Exception, Error) as error:
#         print("Ошибка при работе с PostgreSQL", error)
#     finally:
#         if connection:
#             cursor.close()
#             connection.close()
#
# chectablefromDB11(name_table,newdataD)


##############################
else:
# Если время последней сделки НЕ совпало - заносим инфу в БД и переменную lastdatetimetrade
# Проверяем перед обновлением, не перешли ли торги на новый день после 00:00 часов.
print('Метка 0.1')
a_data = updateloghourse(workrequest[-1]['date'])
b_data = lastdatetimetrade
print(type(a_data))
print(type(b_data))
print('Метка 0.1')
ttl_lastdatetimetrade = a_data - b_data
print('Метка 0.2')
ttl_lastdatetimetrade = str(ttl_lastdatetimetrade)
ttl_lastdatetimetrade = ttl_lastdatetimetrade.split()[0]
print('Метка 1')

# условия
if ttl_lastdatetimetrade <= '-1':
    print('Начался новый торговый день!')
    # создаем таблицу новую.
    create_table_sql(name_table)
    # запись 200 транзакции в новую таблицу со сменой названия уже за текущую дату.
    print('Метка 2')
    firststartreturnhistoryTrade_sql(response, logdatanametech, name_table)
    # Обрезка времени из полной даты в нужной переменной.
    b_data = datetime.date(b_data.year, b_data.month, b_data.day)
    print('Метка 3')
    # запрос к БД на удаление записей ранее нового дня до 00:00ч.
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
        lastdatetimetrade = check_last_date_edit_table(name_table)
        print('Метка 4')


    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()

