import requests
import time
import os.path
from DataFormatCorrection.UpdateData import *

#header
params = (
    ('command', 'returnTradeHistory'),
    ('currencyPair', 'BTC_ETH'),
)
############

check_file = os.path.exists(f'TradeHistory{realdatatame()}.txt')
logdatanamemarket = f'TradeHistory{realdatatame()}.txt'
logdatanametech = f'techlog{realdatatame()}.txt'

if check_file:
    print('file here!')
    # дополнение лога в бесконечном цикле.
    i = 0
    lastdatetimetrade = ''
    while i < 999:
        time.sleep(10)
        pass
        # lastdatetimetrade = cycleupdatelogmarket(lastdatetimetrade, params,logdatanamemarket,logdatanametech)
    print('SCRIPT DONE')
else:
    # marketlog = open(logdatanamemarket, 'w')
    # marketlog.close()

    response = requests.get('https://poloniex.com/public', params=params)
    if (response.status_code):
        lastdatetimetrade = firststartreturnhistoryTrade_sql(response,logdatanametech)

        # дополнение лога в бесконечном цикле.
        i = 0
        while i < 999:
            time.sleep(2)
            lastdatetimetrade = cycleupdatelogmarket_sql(lastdatetimetrade, params,logdatanametech)

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






