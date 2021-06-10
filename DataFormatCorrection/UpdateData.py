import datetime
from psycopg2 import Error
import psycopg2
import time
import requests
from DataFormatCorrection.UpdateData import *

#проводим время из Азорских островов к Москве
def updateloghourse(datafromserver):
    upgradedata = datetime.datetime.strptime(datafromserver, "%Y-%m-%d %H:%M:%S")
    upgradedata = upgradedata + datetime.timedelta(hours=3)
    return upgradedata

def realdatatame():
    t = datetime.datetime.date(datetime.datetime.now())
    #t = t + datetime.timedelta(hours=48) #Увеличение\уменьшение времени
    t = t.strftime('%d_%m_%Y')
    return t

####
def firststartreturnhistoryTrade_sql(response,logdatanametech):
    # Время торгов идет по Азорским остравам.
    requestJSON = ''.join(map(str, response.json()))  # пребразование list в str для нормального чтения в лог.
    outserverlocallog = "GET \nServer status OK\nJSON: " + requestJSON + '\n' + '_' * 60

    # запись в технический лог
    techlog = open(logdatanametech, 'w')
    techlog.write(outserverlocallog)
    techlog.close()

    # logical
    workrequest = response.json()
    workrequest.reverse()  # JSON запрос прилетает списокм от нового к старому. Нам так не нужно бы его переворачиваем.

    sql_json = []
    for i in range(len(workrequest)):
        sql_json.append(workrequest[i]['globalTradeID'])
        sql_json.append(workrequest[i]['tradeID'])
        sql_json.append(workrequest[i]['date'])
        sql_json.append(workrequest[i]['type'])
        sql_json.append(workrequest[i]['rate'])
        sql_json.append(workrequest[i]['amount'])
        sql_json.append(workrequest[i]['total'])

        sqlADDinfoTable(sql_json)
        sql_json.clear()

        if (i == 199):
            lastdatetimetrade = updateloghourse(workrequest[i]['date'])
        else:
            pass

    return lastdatetimetrade


def cycleupdatelogmarket_sql(lastdatetimetrade, params,logdatanametech):
    response = requests.get('https://poloniex.com/public', params=params)
    if (response.status_code):
        requestJSON = ''.join(map(str, response.json()))  # пребразование list в str для нормального чтения.
        outserverlocallog = "\nServer status OK\nJSON: " + requestJSON + '\n' + '_' * 60

        # запись в технический лог
        techlog = open(logdatanametech, 'a')
        techlog.write(outserverlocallog)
        techlog.close()

        workrequest = response.json()
        workrequest.reverse()
        if (updateloghourse(workrequest[-1]['date']) == lastdatetimetrade):
            #Если время совпало с последней записью 200 сделок то - возвращаем последнюю дату сделки без изменения
            print('Время совпало')
            return lastdatetimetrade
        else:
            #Если время последней сделки НЕ совпало - заносим инфу в БД и переменную lastdatetimetrade
            result = ''
            print('время не совпало')
            sql_json = []
            sql_json.append(workrequest[-1]['globalTradeID'])
            sql_json.append(workrequest[-1]['tradeID'])
            sql_json.append(workrequest[-1]['date'])
            sql_json.append(workrequest[-1]['type'])
            sql_json.append(workrequest[-1]['rate'])
            sql_json.append(workrequest[-1]['amount'])
            sql_json.append(workrequest[-1]['total'])

            sqlADDinfoTable(sql_json)
            sql_json.clear()

            # обновление последней даты сделки в переменной lastdatetimetrade для следующего цикла
            lastdatetimetrade = updateloghourse(workrequest[-1]['date'])
            print("Запись успешно обновлена")
            return lastdatetimetrade


#Запись в подготовленную таблицу sql
def sqlADDinfoTable(addinfo):
    try:
        # Подключение к существующей базе данных
        connection = psycopg2.connect(user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password="111111",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="postgres")

        cursor = connection.cursor()
        print(addinfo)
        # Выполнение SQL-запроса для вставки данных в таблицу
        insert_query = f""" INSERT INTO postgreetest_db (globalTradeID, tradeID, date, type, rate, amount, total) VALUES
            (%s,%s,%s,%s,%s,%s,%s)"""

        cursor.execute(insert_query,addinfo)
        connection.commit()
        print("запись успешно вставлена")

        # Получить результат
        cursor.execute("SELECT * from postgreetest_db")
        record = cursor.fetchall()
        print("Результат", record)

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)

    finally:
        if connection:
            cursor.close()
            connection.close()


def chectablefromDB():
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
        insert_query = """SELECT table_name FROM information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'postgres')
    AND table_schema IN('public', 'myschema')"""

        # Получить результат
        cursor.execute(insert_query)
        record = cursor.fetchall()
        print("Результат", record)
        return record

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


def create_table_sql():
    pass