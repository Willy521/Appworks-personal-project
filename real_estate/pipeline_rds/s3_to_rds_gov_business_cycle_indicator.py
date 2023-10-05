# s3_to_rds_gov_economic_cycle_indicator

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
    object_key = 'crawl_to_s3_file/business_cycle_data.json'  # S3文件的名字
    file_name = 'download_from_s3_file/business_cycle_data.json'  # 本地保存的文件名

    # 在本地創建一個資料夾，將JSON file 存入本地資料夾
    directory = "download_from_s3_file"
    if not os.path.exists(directory):
        os.makedirs(directory)

    # S3 download
    download_file_from_s3(bucket_name, object_key, file_name)
    with open(file_name, 'r', encoding='utf-8') as f:
        cycle_indicator = json.load(f)
    print(json.dumps(cycle_indicator, indent=4, ensure_ascii=False))

    # create business cycle indicator db
    conn = connect_to_db()

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS economic_cycle_indicator (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    time_id VARCHAR(255) UNIQUE,
                    time_name VARCHAR(255),
                    leading_indicator FLOAT COMMENT '景氣領先指標綜合指數',
                    leading_indicator_without_trend FLOAT COMMENT '景氣領先指標不含趨勢指數',
                    coincident_indicator FLOAT COMMENT '景氣同時指標綜合指數',
                    coincident_indicator_without_trend FLOAT COMMENT '景氣同時指標不含趨勢指數',
                    strategy_signal INT COMMENT '景氣對策信號'
                )
            """)

            observations_0 = cycle_indicator['data']['dataSets'][0]['series']['0']['observations']
            observations_1 = cycle_indicator['data']['dataSets'][0]['series']['1']['observations']
            observations_2 = cycle_indicator['data']['dataSets'][0]['series']['2']['observations']
            observations_3 = cycle_indicator['data']['dataSets'][0]['series']['3']['observations']
            observations_4 = cycle_indicator['data']['dataSets'][0]['series']['4']['observations']

            time_structure = cycle_indicator['data']['structure']['dimensions']['observation'][0]['values']

            data_to_insert = []

            for idx, time_info in enumerate(time_structure):
                time_id = time_info['id']
                time_name = time_info['name']
                leading_indicator = observations_0[str(idx)][0]
                leading_indicator_without_trend = observations_1[str(idx)][0]
                coincident_indicator = observations_2[str(idx)][0]
                coincident_indicator_without_trend = observations_3[str(idx)][0]
                strategy_signal = observations_4[str(idx)][0]

                data_to_insert.append((
                    time_id, time_name,
                    leading_indicator,
                    leading_indicator_without_trend,
                    coincident_indicator,
                    coincident_indicator_without_trend,
                    strategy_signal
                ))

            cursor.executemany("""
                INSERT INTO economic_cycle_indicator (
                    time_id, time_name,
                    leading_indicator,
                    leading_indicator_without_trend,
                    coincident_indicator,
                    coincident_indicator_without_trend,
                    strategy_signal
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                time_name = VALUES(time_name),
                leading_indicator = VALUES(leading_indicator),
                leading_indicator_without_trend = VALUES(leading_indicator_without_trend),
                coincident_indicator = VALUES(coincident_indicator),
                coincident_indicator_without_trend = VALUES(coincident_indicator_without_trend),
                strategy_signal = VALUES(strategy_signal)
            """, data_to_insert)

            conn.commit()

    finally:
        conn.close()


if __name__ == "__main__":
    main()
