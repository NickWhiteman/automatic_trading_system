from flask import Flask, render_template, jsonify
from threading import Thread
from DataFormatCorrection.jsonrequest_test import test_sql_json
from start import *
from DataFormatCorrection.UpdateData import *
from testpage import *

app = Flask(__name__)

cryptomoney = 'BTC_ETH'
name_table = f'tradehistory_{cryptomoney}_{realdatatame()}'

@app.route('/')
def hello_world():
    return render_template('index.html')

@app.route('/get', methods=['GET'])
def get_otvet():
    sql_select = test_sql_json(name_table)
    return jsonify([sql_select])


# flow1 = Thread(target=app.run)
flow2 = Thread(target=returnTradeHistory, daemon=True)

# flow1.start()
flow2.start()

# if __name__ == '__main__':
#     app.debug = True
#     app.run(host="0.0.0.0", port=5000, debug=True)

