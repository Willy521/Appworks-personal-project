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


# 上傳到S3
def upload_file_to_s3(file_name, bucket):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, file_name)  # 本地的文件路徑跟S3設為一樣
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def crawl_business_cycle_indicator():
    load_dotenv()  # S3環境變數
    start_time = '2000-M1' # Set start_time
    now = datetime.now()  # Get current year and month for end_time
    end_time = now.strftime('%Y-M%m')  # Format it to 'YYYY-MM'
    # print('end_time', end_time)
    response = requests.get(create_url(start_time, end_time))

    if response.status_code == 200:  # 判斷response成功與否
        try:  # 判斷是否是有效的JSON格式
            data = json.loads(response.text)

            print('API Response:')
            print(json.dumps(data, indent=4, ensure_ascii=False))  # Pretty print the output

            # Save as JSON file
            # 在本地創建一個資料夾，將JSON file 存入資料夾並上傳到S3
            directory = "crawl_to_s3_file"
            if not os.path.exists(directory):
                os.makedirs(directory)
            json_file_path = "crawl_to_s3_file/business_cycle_data.json"

            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            # Upload to S3
            bucket_name = 'appworks.personal.project'  # Replace with your bucket name
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


def fetch_data_to_db():
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


# 定義DAG和其默認參數
default_args = {
    'owner': 'Willy',
    'depends_on_past': False,  # 若上一次失敗 這一次還會執行
    'email_on_failure': True,  # 若失敗會發送email給我
    'email_on_retry': True,  # 若設定為 True，當任務重試時將會發送郵件。
    'retries': 1,  # 若任務失敗，會嘗試重跑的次數。
    'retry_delay': timedelta(minutes=5),  # 重試之間的時間間隔
}

dag = DAG(
    'business_cycle_indicator_pipeline',  # DAG 的唯一識別碼
    default_args=default_args,  # 上面定義的默認參數
    description='A pipeline for crawling business_cycle_indicator_pipeline and processing the data.',  # DAG 的描述
    schedule_interval=timedelta(days=1),  # DAG的執行間隔。這裡設定為每天一次
    start_date=datetime(2023, 10, 13),  # DAG 的開始日期
    catchup=False  # 若為 True，則當 DAG 啟動時，將會執行從 start_date 到當前日期之間的所有排程。若為 False，則只會執行最新的排程。
)

t1 = PythonOperator(
    task_id='business_cycle_indicator_upload_to_S3',
    python_callable=crawl_business_cycle_indicator,
    dag=dag,  # 指定該任務屬於哪個 DAG。
)

t2 = PythonOperator(
    task_id='fetch_data_to_db',
    python_callable=fetch_data_to_db,
    dag=dag,
)

t1 >> t2
