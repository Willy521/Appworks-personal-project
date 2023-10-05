# house_real_price

import json
import pymysql
from decouple import config
from dotenv import load_dotenv
import os
import re
import boto3
import csv
import pandas as pd


# 連接RDS DB
def connect_to_db():
    password = config('DATABASE_PASSWORD')

    # 如果try 這條路徑出現異常，就會跳到except
    try:
        conn = pymysql.connect(
            host='appworks.cwjujjrb7yo0.ap-southeast-2.rds.amazonaws.com',
            port=3306,
            user='admin',
            password=password,
            database='estate_data_hub',
            charset='utf8mb4'
        )
        print("Have connected to MySQL")
        return conn
    except Exception as e:  # 抓取所有異常，e是異常的對象
        print(f"Failed to connect to MySQL: {e}")
        return None  # 返回None，代表連接失敗


def download_file_from_s3(bucket_name, object_key, file_name):
    s3 = boto3.client('s3')

    try:
        s3.download_file(bucket_name, object_key, file_name)
        print(f"File downloaded from S3: {file_name}")
        return True
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        return False


def read_csv(file_name):
    # 使用with语句打开文件，这样可以确保文件用完后会被正确关闭
    with open(file_name, mode='r', encoding='utf-8') as file:
        # 创建一个CSV阅读器对象
        csv_reader = csv.reader(file)

        # 遍历CSV文件的每一行
        for row in csv_reader:
            print(row)  # 打印當前內容


def read_csv_pandas(file_name):
    # 使用pandas读取CSV文件
    data = pd.read_csv(file_name)

    # 顯示前幾行數據
    print(data.head())


def create_table(conn):
    try:
        with conn.cursor() as cursor:
            sql = """
            CREATE TABLE IF NOT EXISTS real_estate (
                id INT AUTO_INCREMENT PRIMARY KEY,
                district VARCHAR(255),
                transaction_sign VARCHAR(255),
                address VARCHAR(255),
                land_area FLOAT,
                zoning VARCHAR(255),
                non_metropolis_land_use_district VARCHAR(255),
                non_metropolis_land_use VARCHAR(255),
                transaction_date VARCHAR(255),
                transaction_pen_number VARCHAR(255),
                shifting_level VARCHAR(255),
                total_floor_number INT,
                building_state VARCHAR(255),
                main_use VARCHAR(255),
                main_building_materials VARCHAR(255),
                construction_complete_date VARCHAR(255),
                building_area FLOAT,
                pattern_room INT,
                pattern_hall INT,
                pattern_health INT,
                pattern_compartmented VARCHAR(255),
                has_management_organization VARCHAR(255),
                total_price_NTD BIGINT,
                unit_price_NTD_per_square_meter INT,
                berth_category VARCHAR(255),
                berth_area FLOAT,
                berth_total_price_NTD BIGINT,
                note TEXT,
                serial_number VARCHAR(255),
                build_case VARCHAR(255),
                buildings_and_number VARCHAR(255)
            )
            """
            cursor.execute(sql)
            conn.commit()
            print("Table created successfully")
    except Exception as e:
        print(f"Error creating table: {e}")


def insert_data_from_csv(conn, file_name):
    try:
        with conn.cursor() as cursor, open(file_name, newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile)
            next(csv_reader)  # skip header row
            next(csv_reader)  # skip header row

            sql = """
            INSERT INTO real_estate (
                district, transaction_sign, address, land_area, zoning,
                non_metropolis_land_use_district, non_metropolis_land_use,
                transaction_date, transaction_pen_number, shifting_level, total_floor_number,
                building_state, main_use, main_building_materials, construction_complete_date,
                building_area, pattern_room, pattern_hall, pattern_health, pattern_compartmented,
                has_management_organization, total_price_NTD, unit_price_NTD_per_square_meter,
                berth_category, berth_area, berth_total_price_NTD, note, serial_number,
                build_case, buildings_and_number
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            district=VALUES(district),
            transaction_sign=VALUES(transaction_sign),
            address=VALUES(address),
            land_area=VALUES(land_area),
            zoning=VALUES(zoning),
            non_metropolis_land_use_district=VALUES(non_metropolis_land_use_district),
            non_metropolis_land_use=VALUES(non_metropolis_land_use),
            transaction_date=VALUES(transaction_date),
            transaction_pen_number=VALUES(transaction_pen_number),
            shifting_level=VALUES(shifting_level),
            total_floor_number=VALUES(total_floor_number),
            building_state=VALUES(building_state),
            main_use=VALUES(main_use),
            main_building_materials=VALUES(main_building_materials),
            construction_complete_date=VALUES(construction_complete_date),
            building_area=VALUES(building_area),
            pattern_room=VALUES(pattern_room),
            pattern_hall=VALUES(pattern_hall),
            pattern_health=VALUES(pattern_health),
            pattern_compartmented=VALUES(pattern_compartmented),
            has_management_organization=VALUES(has_management_organization),
            total_price_NTD=VALUES(total_price_NTD),
            unit_price_NTD_per_square_meter=VALUES(unit_price_NTD_per_square_meter),
            berth_category=VALUES(berth_category),
            berth_area=VALUES(berth_area),
            berth_total_price_NTD=VALUES(berth_total_price_NTD),
            note=VALUES(note),
            serial_number=VALUES(serial_number),
            build_case=VALUES(build_case),
            buildings_and_number=VALUES(buildings_and_number)
            """
            # Collect all rows in a list
            data_to_insert = [row for row in csv_reader]

            # Use executemany to insert all data at once
            cursor.executemany(sql, data_to_insert)
        conn.commit()
        print(f"{file_name }Data inserted successfully")
    except Exception as e:
        print(f"{file_name } Error inserting data: {e}")


def main():
    load_dotenv()  # 載入環境變數

    # 定義S3的桶名，對象key和要保存的文件名
    bucket_name = 'appworks.personal.project'
    file_name = 'real_estate_price/last_download.csv'  # 下載後在本地保存的文件

    # 如果沒有就先創資料夾在本地
    if not os.path.exists("real_estate_price"):
        os.makedirs("real_estate_price")

    #  S3 上面的資料
    object_keys = [

        # 桃園
        'real_estate_price/unzipped_112_1/h_lvr_land_b.csv',
        'real_estate_price/unzipped_112_2/h_lvr_land_b.csv',
        'real_estate_price/unzipped_112_3/h_lvr_land_b.csv',

        # 高雄
        'real_estate_price/unzipped_112_1/e_lvr_land_b.csv',
        'real_estate_price/unzipped_112_2/e_lvr_land_b.csv',
        'real_estate_price/unzipped_112_3/e_lvr_land_b.csv',

        # 台南
        'real_estate_price/unzipped_112_1/d_lvr_land_b.csv',
        'real_estate_price/unzipped_112_2/d_lvr_land_b.csv',
        'real_estate_price/unzipped_112_3/d_lvr_land_b.csv',

        # 台中
        'real_estate_price/unzipped_112_1/b_lvr_land_b.csv',
        'real_estate_price/unzipped_112_2/b_lvr_land_b.csv',
        'real_estate_price/unzipped_112_3/b_lvr_land_b.csv',

        # 台北
        'real_estate_price/unzipped_112_1/a_lvr_land_b.csv',
        'real_estate_price/unzipped_112_2/a_lvr_land_b.csv',
        'real_estate_price/unzipped_112_3/a_lvr_land_b.csv',

        # 新北
        'real_estate_price/unzipped_112_1/f_lvr_land_b.csv',
        'real_estate_price/unzipped_112_2/f_lvr_land_b.csv',
        'real_estate_price/unzipped_112_3/f_lvr_land_b.csv',

        # 新竹市
        'real_estate_price/unzipped_112_1/o_lvr_land_b.csv',
        'real_estate_price/unzipped_112_2/o_lvr_land_b.csv',
        'real_estate_price/unzipped_112_3/o_lvr_land_b.csv',

        # 新竹縣
        'real_estate_price/unzipped_112_1/j_lvr_land_b.csv',
        'real_estate_price/unzipped_112_2/j_lvr_land_b.csv',
        'real_estate_price/unzipped_112_3/j_lvr_land_b.csv'

    ]

    conn = connect_to_db()
    if not conn:
        print("Unable to connect to database")
        return

    create_table(conn)

    for object_key in object_keys:
        # S3 download
        if download_file_from_s3(bucket_name, object_key, file_name):
            # 如果文件下载成功，插入数据到数据库
            insert_data_from_csv(conn, file_name)
    conn.close()


if __name__ == "__main__":
    main()