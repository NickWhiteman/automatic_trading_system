import psycopg2
import datetime
import time
from psycopg2 import Error
from DataFormatCorrection.UpdateData import *

# try:
#     # Подключение к существующей базе данных
#     connection = psycopg2.connect(user="postgres",
#                                   # пароль, который указали при установке PostgreSQL
#                                   password="111111",
#                                   host="127.0.0.1",
#                                   port="5432",
#                                   database="postgres")
#     result = [561906993, 52262538, '2021-06-07 21:52:09', 'buy', '0.07642600', '0.01929000', '0.00147425']
#     cursor = connection.cursor()
#     # Выполнение SQL-запроса для вставки данных в таблицу
#     insert_query = f""" INSERT INTO postgreetest_db (globalTradeID, tradeID, date, type, rate, amount, total) VALUES
#     (%s,%s,%s,%s,%s,%s,%s)"""
#
#     cursor.execute(insert_query,result) #тут узкое место, подстановка %s работает от сюда.
#     connection.commit()
#     print("запись успешно вставлена")
#     # Получить результат
#     cursor.execute("SELECT * from postgreetest_db")
#     record = cursor.fetchall()
#     print("Результат", record)
#
# except (Exception, Error) as error:
#     print("Ошибка при работе с PostgreSQL", error)
# finally:
#     if connection:
#         cursor.close()
#         connection.close()
#         print("Соединение с PostgreSQL закрыто")

# try:
#     # Подключение к существующей базе данных
#     connection = psycopg2.connect(user="postgres",
#                                   # пароль, который указали при установке PostgreSQL
#                                   password="111111",
#                                   host="127.0.0.1",
#                                   port="5432",
#                                   database="postgres")
#     cursor = connection.cursor()
#     # Выполнение SQL-запроса для вставки данных в таблицу
#     insert_query = """SELECT table_name FROM information_schema.tables
# WHERE table_schema NOT IN ('information_schema', 'postgres')
# AND table_schema IN('public', 'myschema')"""
#
#     # Получить результат
#     cursor.execute(insert_query)
#     record = cursor.fetchall()
#     print("Результат", record)
#
# except (Exception, Error) as error:
#     print("Ошибка при работе с PostgreSQL", error)
# finally:
#     if connection:
#         cursor.close()
#         connection.close()
#         print("Соединение с PostgreSQL закрыто")


#header
cryptomoney = 'BTC_ETH'
params = (
    ('command', 'returnTradeHistory'),
    ('currencyPair', cryptomoney),
)
############

name_table = f'tradehistory_{cryptomoney}_{realdatatame()}'
name_table = name_table.lower()
logdatanamemarket = f'tradehstory{realdatatame()}.txt'
logdatanametech = f'techlog{realdatatame()}.txt'


check_table_db = chectablefromDB(name_table)
print(check_table_db)
if check_table_db:
    print('file here!')
    # дополнение лога в бесконечном цикле.
    i = 0
    lastdatetimetrade = updateloghourse(check_last_date_edit_table(name_table))

    print(lastdatetimetrade)
    print(type(lastdatetimetrade))
    while True:
        time.sleep(2)
        lastdatetimetrade = cycleupdatelogmarket_sql(lastdatetimetrade, params,logdatanametech,name_table)
    print('SCRIPT DONE')

else:
    print('hueta')


