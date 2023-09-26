# s3_to_rds_gov_economic_construction_cost
import json
import pymysql
from decouple import config
from dotenv import load_dotenv
import boto3


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
    object_key = 'crawl_to_s3/economic_construction_cost_data.json'
    file_name = 'economic_construction_cost_data.json'

    # S3 download
    download_file_from_s3(bucket_name, object_key, file_name)

    with open(file_name, 'r', encoding='utf-8') as f:
        construction_cost = json.load(f)
    print(json.dumps(construction_cost, indent=4, ensure_ascii=False))

    # create business cycle indicator db
    conn = connect_to_db()

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                        CREATE TABLE IF NOT EXISTS economic_construction_cost (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            time_id VARCHAR(255) UNIQUE,
                            time_name VARCHAR(255),
                            construction_index FLOAT COMMENT '營造工程總指數'
                        )
                    """)

            conn.commit()

            observations_0 = construction_cost['data']['dataSets'][0]['series']['0']['observations']
            time_structure = construction_cost['data']['structure']['dimensions']['observation'][0]['values']

            data_to_insert = []
            for idx, time_info in enumerate(time_structure):
                time_id = time_info['id']
                time_name = time_info['name']
                construction_index = observations_0[str(idx)][0]

                data_to_insert.append((time_id, time_name, construction_index))

            cursor.executemany("""
                        INSERT INTO economic_construction_cost (time_id, time_name, construction_index)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        time_name = VALUES(time_name),
                        construction_index = VALUES(construction_index)
                    """, data_to_insert)
            conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
