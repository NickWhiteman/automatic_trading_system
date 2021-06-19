import psycopg2
import datetime
import time
from psycopg2 import Error
from DataFormatCorrection.UpdateData import *
from threading import Thread
from time import sleep
from start import *


# def ga1():
#     for i in range(99):
#         print(i)
#         sleep(0.5)
#
#
# th1 = Thread(target=ga1, daemon=True)
# th2 = Thread(target=returnTradeHistory, daemon=True)
# th1.start()
# th2.start()
# def ga2():
#     for i in range(200,700):
#         print(i)
#         sleep(1)
# ga2()

# returnTradeHistory()


