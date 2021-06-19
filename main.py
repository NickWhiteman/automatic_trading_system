from flask import Flask, render_template,jsonify
from threading import Thread
from DataFormatCorrection.jsonrequest_test import test_sql_json
from start import *
from DataFormatCorrection.UpdateData import *

app = Flask(__name__)

cryptomoney = 'BTC_ETH'
name_table = f'tradehistory_{cryptomoney}_{realdatatame()}'

# Запуск потока returnTradeHistory демоном на постоянное обновление таблицы
flow1 = Thread(target=returnTradeHistory, daemon=True)
flow1.start()

@app.route('/')
def hello_world():
    return render_template('index.html')

@app.route('/get', methods=['GET'])
def get_otvet():
    sql_select = test_sql_json(name_table)
    return jsonify([sql_select])

if __name__ == '__main__':
    app.debug = True
    app.run()
87180