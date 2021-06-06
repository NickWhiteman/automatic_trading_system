import datetime
import time
import requests

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
            transform = workrequest[-1]['date']
            result += f'date: {updateloghourse(transform)}\n'
            transform = workrequest[-1]['globalTradeID']
            result += f'globalTradeID: {transform}\n'
            transform = workrequest[-1]['type']
            result += f'type: {transform}\n'
            transform = workrequest[-1]['rate']
            result += f'rate: {transform}\n'
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