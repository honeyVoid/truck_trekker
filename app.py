from flask import Flask, render_template, request

from flask_sqlalchemy import SQLAlchemy

import requests as r

import json as j

import mysql.connector

from datetime import datetime, timedelta


import gspread
from gspread.exceptions import APIError
from gspread import Client, Worksheet, Spreadsheet

import time

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqlconnector://root:55662233@localhost/truck_trekker'
db = SQLAlchemy(app)


def hhhh(
        data_list: list[dict],
        center_cords: tuple[float, float],
        radius: float
):
    """
    Парсит данные и при помощи встроенных
    методов базы данных определяет
    находится ли точка в заданных координатах,
    затем записывает все в таблицу Google Sheets.
    """
    for data in data_list:
        sireal = data[0].get('Serial')
        print(sireal)
        count = 0
        print(data)

        db_def = mysql.connector.connect(
            host='localhost',
            user='root',
            password='55662233',
            database=''
        )
        cursor = db_def.cursor()
        query = (
            "SELECT "
            "IF(ST_Distance_Sphere(POINT(%s, %s), POINT(%s, %s)) <= %s * 1000, 'Inside', 'Outside') AS status"
        )

        for d in data:
            try:
                point_cords = (d.get('Latitude'), d.get('Longitude'))
                cursor.execute(query, (*point_cords, *center_cords, radius))
                result = cursor.fetchone()
                if result[0] == 'Inside':
                    count += 1
            except Exception as error:
                print(error)

        cursor.close()
        db_def.close()
        print(count)

        if count > 0:
            rows_to_append = []
            for truck in TruckInfo.query.filter_by(sireal=sireal).all():
                row = [sireal, truck.phone_number, truck.plate, count]
                rows_to_append.append(row)
            print(rows_to_append)

            retry_count = 3  
            for retry in range(retry_count):
                try:
                    gc: Client = gspread.service_account('service_account.json')
                    sh: Spreadsheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1FGRPjD7_GxFSaLEP7T56WhMEgOle7VaKvhZ6eqK_RXk/')
                    ws: Worksheet = sh.sheet1
                    ws.append_rows(rows_to_append)
                    break
                except APIError as api_error:
                    print(f"Ошибка при работе с Google Sheets API: {api_error}")
                    if retry < retry_count - 1:
                        print(f"Попытка №{retry + 2}...")
                        time.sleep(10)
                    else:
                        print("Превышено количество попыток. Запись не выполнена.")
                except Exception as other_error:
                    print(f"Произошла ошибка: {other_error}")


class TruckInfo(db.Model):
    """модель для информации о устройстве"""
    id = db.Column(db.Integer, primary_key=True)
    sireal = db.Column(db.Integer, nullable=False)
    plate = db.Column(db.String(45), nullable=False)
    phone_number = db.Column(db.String(45), nullable=False)


def get_sireal_from_db() -> list:
    """Получения списка обьктов TruckInfo"""
    sireal_list = []
    with app.app_context():
        trucks = TruckInfo.query.all()
        for truck in trucks:
            sireal_list.append(truck)
        print(len(sireal_list))
        return sireal_list


def get_api_data(sireal_list: list, date_list: list) -> list[list[dict]]:
    """Возвращает на кажжой итерации список с данными с апи"""
    for date in date_list:
        for sireal in sireal_list:
            res = r.get(
                f'http://51.250.1.53/api/Track?serial={sireal.sireal}&date={date}'
            )
            try:
                rrr = res.json()
                data = j.loads(rrr)
                if data:
                    yield data
                else:
                    pass
            except TypeError:
                print(res)


def to_unix(start_date: str, end_date: str) -> list:
    """
    Конвертирует время в unix и возвращает
    список со всеми датами в указанном промежутке"
    """
    date_format = '%Y-%m-%d'
    start_datetime = datetime.strptime(start_date, date_format)
    end_datetime = datetime.strptime(end_date, date_format)

    date_list = []
    current_date = start_datetime
    while current_date <= end_datetime:
        unix_timestamp = int(current_date.timestamp())
        date_list.append(unix_timestamp)
        current_date += timedelta(days=1)
    return date_list


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        return render_template('index.html')
    if request.method == 'POST' and 'search' in request.form:

        latitude = request.form['latitude']
        longitude = request.form['longitude']
        radius = request.form['radius']
        date_list = to_unix(
            start_date=request.form['start_date'],
            end_date=request.form['end_date']
        )
        sireal_list_from_db = get_sireal_from_db()
        lat_long = get_api_data(
            sireal_list=sireal_list_from_db,
            date_list=date_list
        )
        print(latitude, longitude)
        hhhh(
            data_list=lat_long,
            center_cords=(
                float(latitude),
                float(longitude),
            ),
            radius=radius
        )
        return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True)
