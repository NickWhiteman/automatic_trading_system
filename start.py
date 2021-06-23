import requests
import time
import os.path
from DataFormatCorrection.UpdateData import *

def returnTradeHistory():
    #header
    cryptomoney = 'BTC_ETH'
    params = (
        ('command', 'returnTradeHistory'),
        ('currencyPair', cryptomoney),
    )
    ############
    name_table = f'tradehistory_{cryptomoney}_{realdatatame()}'
    name_table = name_table.lower()
    # logdatanamemarket = f'tradehstory{realdatatame()}.txt'
    logdatanametech = f'techlog{realdatatame()}.txt'

    #Проверка существования таблицы за текущую дату.
    check_table_db = chectablefromDB(name_table)

    if check_table_db:
        print('file here!')
        # дополнение лога в бесконечном цикле.
        # #Проверка даты последней записи
        lastdatetimetrade = update_str_for_datatime(check_last_date_edit_table(name_table))

        while True:
            # Блок обновления дат
            name_table = f'tradehistory_{cryptomoney}_{realdatatame()}'
            name_table = name_table.lower()
            logdatanametech = f'techlog{realdatatame()}.txt'
            print('Последняя дата')
            print(lastdatetimetrade)
            lastdatetimetrade = cycleupdatelogmarket_sql(lastdatetimetrade, params,logdatanametech,name_table)
            time.sleep(2)

    else:
        # marketlog = open(logdatanamemarket, 'w')
        # marketlog.close()
        print('Файла нет!')
        response = requests.get('https://poloniex.com/public', params=params)
        if (response.status_code):
            #Создание новой таблицы
            create_table_sql(name_table)
            print('Метка 5-1')
            #Первичное наполнение таблицы
            lastdatetimetrade = firststartreturnhistoryTrade_sql(response,logdatanametech, name_table)
            print('Метка 5-2')
            # дополнение лога в бесконечном цикле.

            # Вывод последней записи в таблице на новый цикл.
            firs_edit_data = update_str_for_datatime(check_last_date_edit_table(name_table))
            # Преобразование даты в сокращенный вид. Пример: 2021-06-24
            f_data = datetime.date(firs_edit_data.year, firs_edit_data.month, firs_edit_data.day)
            #Удаление дат до указанной при создании таблицы.
            clear_date_before(name_table, f_data)

            while True:
                # Блок обновления дат
                name_table = f'tradehistory_{cryptomoney}_{realdatatame()}'
                name_table = name_table.lower()
                logdatanametech = f'techlog{realdatatame()}.txt'
                print('Метка 5-3')
                time.sleep(2)
                lastdatetimetrade = cycleupdatelogmarket_sql(lastdatetimetrade, params,logdatanametech,name_table)
                print('Метка 5-4')
            print('SCRIPT DONE')
        else:
            print("Error server response: " + response.status_code)
            print('_' * 60)
            requestJSON = ''.join(map(str, response.json()))  # пребразование list в str для нормального чтения в лог.
            outserverlocallog = "GET \nServer status NOT\nJSON: " + requestJSON + '\n' + '_' * 60

            # запись в технический лог
            techlog = open(logdatanametech, 'w')
            techlog.write(outserverlocallog)
            techlog.close()





