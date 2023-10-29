# business_cycle_indicator_dag

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


def create_url(start_time, last_date_queried):  # create
    api_base_url = 'https://nstatdb.dgbas.gov.tw/dgbasAll/webMain.aspx?sdmx/A120101010/1+2+3+4+5.1.1.M/'
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


def crawl_business_cycle_indicator():
    load_dotenv()
    start_time = '2000-M1'
    now = datetime.now()
    end_time = now.strftime('%Y-M%m')  # Format it to 'YYYY-MM'
    response = requests.get(create_url(start_time, end_time))

    if response.status_code == 200:
        try:
            data = json.loads(response.text)

            print('API Response:')
            print(json.dumps(data, indent=4, ensure_ascii=False))  # Pretty print the output

            # Save as JSON file
            directory = "crawl_to_s3_file"
            if not os.path.exists(directory):
                os.makedirs(directory)
            json_file_path = "crawl_to_s3_file/business_cycle_data.json"

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


# connect to db
def connect_to_db():
    host = config('HOST')
    port = int(config('PORT'))
    user = config('USER')
    database = config('DATABASE')
    password = config('DATABASE_PASSWORD')
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user='admin',
            password=password,
            database=database,
            charset='utf8mb4'
            # connection_timeout=57600
        )
        print("Have connected to db")
        return conn
    except Exception as e:
        print(f"error: {e}")
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


def fetch_data_to_db():
    load_dotenv()
    bucket_name = 'appworks.personal.project'
    object_key = 'crawl_to_s3_file/business_cycle_data.json'
    file_name = 'download_from_s3_file/business_cycle_data.json'

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


default_args = {
    'owner': 'Willy',
    'depends_on_past': False,
    'email': ['r94040119@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'business_cycle_indicator_pipeline',
    default_args=default_args,
    description='A pipeline for crawling business_cycle_indicator_pipeline and processing the data.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 13),
    catchup=False
)

t1 = PythonOperator(
    task_id='business_cycle_indicator_upload_to_S3',
    python_callable=crawl_business_cycle_indicator,
    dag=dag,
)

t2 = PythonOperator(
    task_id='fetch_data_to_db',
    python_callable=fetch_data_to_db,
    dag=dag,
)

t1 >> t2
