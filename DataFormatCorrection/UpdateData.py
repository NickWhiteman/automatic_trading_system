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

def firststartreturnhistoryTrade(response,logdatanamemarket,logdatanametech):
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
    result = ''
    for i in range(len(workrequest)):
        transform = workrequest[i]['date']
        result += f'date: {updateloghourse(transform)}\n'
        transform = workrequest[i]['globalTradeID']
        result += f'globalTradeID: {transform}\n'
        transform = workrequest[i]['type']
        result += f'type: {transform}\n'
        transform = workrequest[i]['rate']
        result += f'rate: {transform}\n'
        result += '_____' * 10
        result += '\n'

        if (i == 199):

            lastdatetimetrade = updateloghourse(workrequest[i]['date'])
        else:
            pass

        # запись в лог market
        marketlog = open(logdatanamemarket, 'a')
        marketlog.write(result)
        marketlog.close()

    return lastdatetimetrade

def cycleupdatelogmarket(lastdatetimetrade, params,logdatanamemarket,logdatanametech):
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
            print('Время совпало')
            return lastdatetimetrade
        else:
            # обновление последней записи в БД
            result = ''
            print('время не совпало')
            transform = workrequest[-1]['globalTradeID']
            result += f'globalTradeID: {transform}\n'
            transform = workrequest[-1]['tradeID']
            result += f'tradeID: {transform}\n'
            transform = workrequest[-1]['date']
            result += f'date: {updateloghourse(transform)}\n'
            transform = workrequest[-1]['type']
            result += f'type: {transform}\n'
            transform = workrequest[-1]['rate']
            result += f'rate: {transform}\n'
            transform = workrequest[-1]['amount']
            result += f'amount: {transform}\n'
            transform = workrequest[-1]['total']
            result += f'total: {transform}\n'
            result += 'ДОПОЛНЕНО!\n'
            result += '_____' * 10
            result += '\n'

            # запись в лог market
            marketlog = open(logdatanamemarket, 'a')
            marketlog.write(result)
            marketlog.close()

            # обновление последней даты сделки
            lastdatetimetrade = updateloghourse(workrequest[-1]['date'])
            print("Запись успешно обновлена")
            return lastdatetimetrade

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

    result = ''
    sql_result = []
    for i in range(len(workrequest)):
        sql_result.append(workrequest[i]['globalTradeID'])
        sql_result.append(workrequest[i]['tradeID'])
        sql_result.append(workrequest[i]['date'])
        sql_result.append(workrequest[i]['type'])
        sql_result.append(workrequest[i]['rate'])
        sql_result.append(workrequest[i]['amount'])
        sql_result.append(workrequest[i]['total'])

        # transform = workrequest[i]['globalTradeID']
        # result += f'{transform}, '
        # transform = workrequest[i]['tradeID']
        # result += f'{transform}, '
        # transform = workrequest[i]['date']
        # result += f'{updateloghourse(transform)}, '
        # transform += workrequest[i]['type']
        # result = f'{transform}, '
        # transform += workrequest[i]['rate']
        # result = f'{transform}, '
        # transform += workrequest[i]['amount']
        # result = f'{transform}, '
        # transform += workrequest[i]['total']
        # result = f'{transform}, '
        # print(result)
        print(sql_result)
        for a in sql_result:
            print(type(a))

        # sqlADDinfoTable(result)
        sql_result.clear()

        if (i == 199):
            lastdatetimetrade = updateloghourse(workrequest[i]['date'])
        else:
            pass

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
        insert_query = f""" INSERT INTO postgreetest_db (date, globalTradeID, type, rate) VALUES ({addinfo})"""

        cursor.execute(insert_query)
        connection.commit()
        print("1 запись успешно вставлена")
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
            print("Соединение с PostgreSQL закрыто")
