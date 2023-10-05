# 五代行庫平均房貸利率insert RDS

import json
import pymysql
from decouple import config
from dotenv import load_dotenv
import boto3
import os


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


def main():
    load_dotenv()
    # 定義S3的桶名，對象key和要保存的文件名
    bucket_name = 'appworks.personal.project'
    object_key = 'crawl_to_s3_file/mortgage_interest_rates.json'  # S3文件的名字
    file_name = "download_from_s3_file/mortgage_interest_rates.json"  # 本地保存的文件名

    # 在本地創建一個資料夾，將JSON file 存入本地資料夾
    directory = "download_from_s3_file"
    if not os.path.exists(directory):
        os.makedirs(directory)

    # S3 download
    download_file_from_s3(bucket_name, object_key, file_name)

    with open(file_name, 'r', encoding='utf-8') as f:
        mortgage_interest_rates = json.load(f)
    print(json.dumps(mortgage_interest_rates, indent=4, ensure_ascii=False))

    # create mortgage rates db
    conn = connect_to_db()
    try:
        with conn.cursor() as cursor:
            data_list = [(data['time'], data['rate']) for data in mortgage_interest_rates]

            sql = """
                INSERT INTO mortgage_interest_rates (period, rate)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE rate = VALUES(rate)
            """

            cursor.executemany(sql, data_list)
        conn.commit()
        print("insert successfully")
    except Exception as e:
        print(f"Error inserting or updating data: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()