from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import os
from datetime import datetime
import json
import pymysql
from decouple import config
from dotenv import load_dotenv
import boto3


def create_url(start_time, last_date_queried):
    api_base_url = 'https://nstatdb.dgbas.gov.tw/dgbasAll/webMain.aspx?sdmx/A018101010/1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.1.1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.Ｑ/'
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


def crawl_gdp():
    load_dotenv()  # S3環境變數

    # 求得當前季度
    month = datetime.now().month
    year = datetime.now().year
    if month <= 3:
        current_quarter = f"{year}-Q1"
    elif month <= 6:
        current_quarter = f"{year}-Q2"
    elif month <= 9:
        current_quarter = f"{year}-Q3"
    else:
        current_quarter = f"{year}-Q4"
    # 發送GET請求
    response = requests.get(create_url('2000-Q1', current_quarter))

    if response.status_code == 200:
        try:
            data = json.loads(response.text)

            print('API Response:')
            print(json.dumps(data, indent=4, ensure_ascii=False))  # Pretty print the output

            # Save as JSON file
            # 在本地創建一個資料夾，將JSON file 存入資料夾並上傳到S3
            directory = "crawl_to_s3_file"
            if not os.path.exists(directory):
                os.makedirs(directory)
            json_file_path = "crawl_to_s3_file/gdp_data.json"  # 存到S3的crawl_to_s3資料夾

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


def process_gdp():
    load_dotenv()
    # 定義S3的桶名，對象key和要保存的文件名
    bucket_name = 'appworks.personal.project'
    object_key = 'crawl_to_s3_file/gdp_data.json'  # S3文件的名字
    file_name = 'download_from_s3_file/gdp_data.json'  # 本地保存的文件名

    # S3 download
    download_file_from_s3(bucket_name, object_key, file_name)

    with open(file_name, 'r', encoding='utf-8') as f:
        gdp_indicator = json.load(f)
    print(json.dumps(gdp_indicator, indent=4, ensure_ascii=False))

    # create business cycle indicator db
    conn = connect_to_db()

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS economic_gdp_indicator (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    time_id VARCHAR(255) UNIQUE,
                    time_name VARCHAR(255),
                    mid_year_population INT COMMENT '期中人口(人)',
                    average_exchange_rate FLOAT COMMENT '平均匯率(元/美元)',
                    economic_growth_rate FLOAT COMMENT '經濟成長率(%)',
                    gdp_million_nyd FLOAT COMMENT '國內生產毛額GDP(名目值，百萬元)',
                    gdp_million_usd FLOAT COMMENT '國內生產毛額GDP(名目值，百萬美元)',
                    gdp_per_capita_nyd FLOAT COMMENT '平均每人GDP(名目值，元)',
                    gdp_per_capita_usd FLOAT COMMENT '平均每人GDP(名目值，美元)',
                    gni_million_nyd FLOAT COMMENT '國民所得毛額GNI(名目值，百萬元)',
                    gni_million_usd FLOAT COMMENT '國民所得毛額GNI(名目值，百萬美元)',
                    gni_per_capita_nyd FLOAT COMMENT '平均每人GNI(名目值，元)',
                    gni_per_capita_usd FLOAT COMMENT '平均每人GNI(名目值，美元)',
                    national_income_million_nyd FLOAT COMMENT '國民所得(名目值，百萬元)',
                    national_income_million_usd FLOAT COMMENT '國民所得(名目值，百萬美元)',
                    income_per_capita_nyd FLOAT COMMENT '平均每人所得(名目值，元)',
                    income_per_capita_usd FLOAT COMMENT '平均每人所得(名目值，美元)'
                )
            """)

            observations_0 = gdp_indicator['data']['dataSets'][0]['series']['0']['observations']
            observations_1 = gdp_indicator['data']['dataSets'][0]['series']['1']['observations']
            observations_2 = gdp_indicator['data']['dataSets'][0]['series']['2']['observations']
            observations_3 = gdp_indicator['data']['dataSets'][0]['series']['3']['observations']
            observations_4 = gdp_indicator['data']['dataSets'][0]['series']['4']['observations']
            observations_5 = gdp_indicator['data']['dataSets'][0]['series']['5']['observations']
            observations_6 = gdp_indicator['data']['dataSets'][0]['series']['6']['observations']
            observations_7 = gdp_indicator['data']['dataSets'][0]['series']['7']['observations']
            observations_8 = gdp_indicator['data']['dataSets'][0]['series']['8']['observations']
            observations_9 = gdp_indicator['data']['dataSets'][0]['series']['9']['observations']
            observations_10 = gdp_indicator['data']['dataSets'][0]['series']['10']['observations']
            observations_11 = gdp_indicator['data']['dataSets'][0]['series']['11']['observations']
            observations_12 = gdp_indicator['data']['dataSets'][0]['series']['12']['observations']
            observations_13 = gdp_indicator['data']['dataSets'][0]['series']['13']['observations']
            observations_14 = gdp_indicator['data']['dataSets'][0]['series']['14']['observations']

            time_structure = gdp_indicator['data']['structure']['dimensions']['observation'][0]['values']

            data_to_insert = []
            for idx, time_info in enumerate(time_structure):
                time_id = time_info['id']
                time_name = time_info['name']
                mid_year_population = observations_0[str(idx)][0]
                average_exchange_rate = observations_1[str(idx)][0]
                economic_growth_rate = observations_2[str(idx)][0]
                gdp_million_nyd = observations_3[str(idx)][0]
                gdp_million_usd = observations_4[str(idx)][0]
                gdp_per_capita_nyd = observations_5[str(idx)][0]
                gdp_per_capita_usd =observations_6[str(idx)][0]
                gni_million_nyd = observations_7[str(idx)][0]
                gni_million_usd = observations_8[str(idx)][0]
                gni_per_capita_nyd = observations_9[str(idx)][0]
                gni_per_capita_usd = observations_10[str(idx)][0]
                national_income_million_nyd = observations_11[str(idx)][0]
                national_income_million_usd = observations_12[str(idx)][0]
                income_per_capita_nyd = observations_13[str(idx)][0]
                income_per_capita_usd = observations_14[str(idx)][0]

                data_to_insert.append((
                    time_id, time_name, mid_year_population, average_exchange_rate,
                    economic_growth_rate, gdp_million_nyd, gdp_million_usd,
                    gdp_per_capita_nyd, gdp_per_capita_usd, gni_million_nyd, gni_million_usd,
                    gni_per_capita_nyd, gni_per_capita_usd, national_income_million_nyd,
                    national_income_million_usd, income_per_capita_nyd, income_per_capita_usd
                ))

            insert_query = """
                INSERT INTO economic_gdp_indicator (
                    time_id, time_name, mid_year_population, average_exchange_rate,
                    economic_growth_rate, gdp_million_nyd, gdp_million_usd,
                    gdp_per_capita_nyd, gdp_per_capita_usd, gni_million_nyd, gni_million_usd,
                    gni_per_capita_nyd, gni_per_capita_usd, national_income_million_nyd,
                    national_income_million_usd, income_per_capita_nyd, income_per_capita_usd
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    time_name = VALUES(time_name), 
                    mid_year_population = VALUES(mid_year_population),
                    average_exchange_rate = VALUES(average_exchange_rate), 
                    economic_growth_rate = VALUES(economic_growth_rate),
                    gdp_million_nyd = VALUES(gdp_million_nyd),
                    gdp_million_usd = VALUES(gdp_million_usd),
                    gdp_per_capita_nyd = VALUES(gdp_per_capita_nyd),
                    gdp_per_capita_usd = VALUES(gdp_per_capita_usd),
                    gni_million_nyd = VALUES(gni_million_nyd),
                    gni_million_usd = VALUES(gni_million_usd),
                    gni_per_capita_nyd = VALUES(gni_per_capita_nyd),
                    gni_per_capita_usd = VALUES(gni_per_capita_usd),
                    national_income_million_nyd = VALUES(national_income_million_nyd),
                    national_income_million_usd = VALUES(national_income_million_usd),
                    income_per_capita_nyd = VALUES(income_per_capita_nyd),
                    income_per_capita_usd = VALUES(income_per_capita_usd)
            """
            cursor.executemany(insert_query, data_to_insert)
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
    'gdp_pipeline',  # DAG 的唯一識別碼
    default_args=default_args,  # 上面定義的默認參數
    description='A pipeline for crawling gdp and processing the data.',  # DAG 的描述
    schedule_interval=timedelta(days=1),  # DAG的執行間隔。這裡設定為每天一次
    start_date=datetime(2023, 10, 13),  # DAG 的開始日期
    catchup=False  # 若為 True，則當 DAG 啟動時，將會執行從 start_date 到當前日期之間的所有排程。若為 False，則只會執行最新的排程。
)

t1 = PythonOperator(
    task_id='gdp_upload_to_S3',
    python_callable=crawl_gdp,
    dag=dag,  # 指定該任務屬於哪個 DAG。
)

t2 = PythonOperator(
    task_id='fetch_gdp_data_to_db',
    python_callable=process_gdp,
    dag=dag,
)

t1 >> t2
