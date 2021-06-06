import requests
import datetime
from DataFormatCorrection.UpdateData import *
import time
import os.path

# params = (
#     ('command', 'returnTradeHistory'),
#     ('currencyPair', 'BTC_ETH'),
# )
# #Время торгов идет по Азорским остравам.
# response = requests.get('https://poloniex.com/public', params=params)
# hh = response.json()
#
#
# print(hh[2]['date'])
# h1 = hh[2]['date']
#
# h2 = datetime.datetime.strptime(h1,"%Y-%m-%d %H:%M:%S")
# h2 = h2 + datetime.timedelta(hours=3)
# print(h2)
#
# print(updateloghourse(hh[2]['date']))

# g1 = datetime.datetime.date(datetime.datetime.now())
# g1 = g1 + datetime.timedelta(hours=48)
# print(datetime.datetime.now())
# print(g1)
# # g2 = datetime.datetime.strptime(g1,"%Y_%m_%d")
# t = datetime.datetime.date(datetime.datetime.now())
# t2 = t + datetime.timedelta(hours=-48)
# t2 = t2.strftime('%d_%m_%Y')
# t = t.strftime('%d_%m_%Y')
#
# print(t)
# print(t2)

print(realdatatame())

check_file = os.path.exists(f'TradeHistory{realdatatame()}.txt')
if check_file:
    print('file here!')
else:
    print('NOT FILE')
    techlog = open(f'TradeHistory{realdatatame()}.txt', 'w')

    techlog.close()

print(f'TradeHistory{realdatatame()}.txt')
print(check_file)

logdataname = f'TradeHistory{realdatatame()}.txt'
print(logdataname)


