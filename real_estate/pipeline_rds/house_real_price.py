# house_real_price
import json
import pymysql
from decouple import config
from dotenv import load_dotenv
import openai
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
            print(row)  # 打印当前行的内容


def read_csv_pandas(file_name):
    # 使用pandas读取CSV文件
    data = pd.read_csv(file_name)

    # 显示前几行数据
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
            """

            # 将所有的行存储在列表中，然后使用executemany一次性插入所有行
            rows = [row for row in csv_reader]
            cursor.executemany(sql, rows)

        conn.commit()
        print("Data inserted successfully")
    except Exception as e:
        print(f"Error inserting data: {e}")


def main():
    load_dotenv()
    # 定義S3的桶名，對象key和要保存的文件名
    bucket_name = 'appworks.personal.project'
    file_name = 'f_lvr_land_b.csv'
    object_keys = [
        # 'real_estate_price/unzipped_111_1/f_lvr_land_b.csv',
        # 'real_estate_price/unzipped_111_2/f_lvr_land_b.csv',
        # 'real_estate_price/unzipped_111_3/f_lvr_land_b.csv',
        # 'real_estate_price/unzipped_111_4/f_lvr_land_b.csv',
        'real_estate_price/unzipped_112_1/f_lvr_land_b.csv',
        'real_estate_price/unzipped_112_2/f_lvr_land_b.csv'
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








