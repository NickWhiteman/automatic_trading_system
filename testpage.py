import datetime
from psycopg2 import Error
import psycopg2
import time
import requests

name_table = 'test_table_docker1234'
# Создание новой таблицы с именем дня трейда и валюты.
def create_table_sql(name_table):
    try:
        # Подключение к существующей базе данных
        connection = psycopg2.connect(user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password="111111",
                                      host="db",
                                      port="5432",
                                      database="postgres")

        cursor = connection.cursor()
        # Выполнение SQL-запроса для вставки данных в таблицу
        insert_query = f"""CREATE TABLE {name_table} (
        globaltradeid         int8,
        tradeid        int8,
        date 			varchar,
        type		varchar,
        rate 		float4,
        amount		float4,
        total		float4
    )"""

        cursor.execute(insert_query)  # Создание таблицы
        connection.commit()
        print(f"Таблица успешно {name_table} создана")


    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


