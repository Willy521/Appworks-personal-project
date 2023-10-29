from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
import pymysql
from decouple import config
from dotenv import load_dotenv
import boto3
import os


def create_url(start_time, last_date_queried):
    api_base_url = 'https://nstatdb.dgbas.gov.tw/dgbasAll/webMain.aspx?sdmx/A030501015/1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.1.1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.M/'
    updated_url = f"{api_base_url}&startTime={start_time}&endTime={last_date_queried}"
    return updated_url


def upload_file_to_s3(file_name, bucket):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, file_name)  # 本地的文件路徑跟S3設為一樣
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False



def connect_to_db():
    password = config('DATABASE_PASSWORD')

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
    except Exception as e:
        print(f"Failed to connect to MySQL: {e}")
        return None


def download_file_from_s3(bucket_name, object_key, file_name):
    s3 = boto3.client('s3')

    try:
        s3.download_file(bucket_name, object_key, file_name)
        print(f"File downloaded from S3: {file_name}")
        return True
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        return False


def crawl_construction_cost():
    load_dotenv()

    start_time = '2000-M1'
    now = datetime.now()
    end_time = now.strftime('%Y-M%m')
    response = requests.get(create_url(start_time, end_time))

    if response.status_code == 200:
        try:
            data = json.loads(response.text)

            print('API Response:')
            print(json.dumps(data, indent=4, ensure_ascii=False))
            # Save as JSON file
            directory = "crawl_to_s3_file"
            if not os.path.exists(directory):
                os.makedirs(directory)
            json_file_path = "crawl_to_s3_file/construction_cost_data.json"

            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            # Upload to S3
            bucket_name = 'appworks.personal.project'
            if upload_file_to_s3(json_file_path, bucket_name):
                print("JSON file successfully uploaded to S3.")
            else:
                print("Failed to upload JSON file to S3.")

        except json.JSONDecodeError:
            print("Received data isn't a valid JSON. Printing raw data:")
            print(response.text)
    else:
        print(f"Failed to get data: {response.status_code}")
        return None


def fetch_construction_cost_data_to_db():
    load_dotenv()
    bucket_name = 'appworks.personal.project'
    object_key = 'crawl_to_s3_file/construction_cost_data.json'
    file_name = 'download_from_s3_file/construction_cost_data.json'

    directory = "download_from_s3_file"
    if not os.path.exists(directory):
        os.makedirs(directory)

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



default_args = {
    'owner': 'Willy',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'construction_cost_pipeline',
    default_args=default_args,
    description='A pipeline for crawling construction_cost_dag and processing the data.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 13),
    catchup=False
)

t1 = PythonOperator(
    task_id='business_construction_cost_upload_to_S3',
    python_callable=crawl_construction_cost,
    dag=dag,
)

t2 = PythonOperator(
    task_id='fetch_construction_cost_data_to_db',
    python_callable=fetch_construction_cost_data_to_db,
    dag=dag,
)

t1 >> t2
