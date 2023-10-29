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
    api_base_url = 'https://nstatdb.dgbas.gov.tw/dgbasAll/webMain.aspx?sdmx/A130201010/1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.1.1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.M/'
    updated_url = f"{api_base_url}&startTime={start_time}&endTime={last_date_queried}"
    return updated_url


def upload_file_to_s3(file_name, bucket):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, file_name)
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


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


def crawl_population():
    load_dotenv()

    start_time = '2000-M1' # Set start_time
    now = datetime.now()  # Get current year and month for end_time
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
            json_file_path = "crawl_to_s3_file/population_data.json"  # 存到S3的crawl_to_s3資料夾

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


def process_population_data():
    load_dotenv()
    bucket_name = 'appworks.personal.project'
    object_key = 'crawl_to_s3_file/population_data.json'
    file_name = 'download_from_s3_file/population_data.json'

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
                CREATE TABLE IF NOT EXISTS society_population_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    time_id VARCHAR(255) UNIQUE,
                    time_name VARCHAR(255),
                    land_area_square_km FLOAT COMMENT '土地面積(平方公里)',
                    number_of_townships FLOAT COMMENT '鄉鎮市區數',
                    number_of_villages FLOAT COMMENT '村里數',
                    number_of_neighborhoods FLOAT COMMENT '鄰數',
                    household_count FLOAT COMMENT '戶數(戶)',
                    population_count FLOAT COMMENT '人口數(人)',
                    population_growth_rate FLOAT COMMENT '人口增加率(‰)',
                    male_population_count FLOAT COMMENT '男性人口數(人)',
                    female_population_count FLOAT COMMENT '女性人口數(人)',
                    gender_ratio FLOAT COMMENT '人口性比例(每百女子所當男子數)',
                    average_household_size FLOAT COMMENT '戶量(人/戶)',
                    population_density_per_square_km FLOAT COMMENT '人口密度(人/平方公里)'
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

            time_structure = gdp_indicator['data']['structure']['dimensions']['observation'][0]['values']

            data_to_insert = []
            for idx, time_info in enumerate(time_structure):
                time_id = time_info['id']
                time_name = time_info['name']
                land_area_square_km = observations_0[str(idx)][0]
                number_of_townships = observations_1[str(idx)][0]
                number_of_villages = observations_2[str(idx)][0]
                number_of_neighborhoods = observations_3[str(idx)][0]
                household_count = observations_4[str(idx)][0]
                population_count = observations_5[str(idx)][0]
                population_growth_rate = observations_6[str(idx)][0]
                male_population_count = observations_7[str(idx)][0]
                female_population_count = observations_8[str(idx)][0]
                gender_ratio = observations_9[str(idx)][0]
                average_household_size = observations_10[str(idx)][0]
                population_density_per_square_km = observations_11[str(idx)][0]

                data_to_insert.append((
                    time_id, time_name, land_area_square_km, number_of_townships, number_of_villages,
                    number_of_neighborhoods, household_count, population_count, population_growth_rate,
                    male_population_count, female_population_count, gender_ratio, average_household_size,
                    population_density_per_square_km
                ))

            upsert_query = """
            INSERT INTO society_population_data (
                time_id, time_name, land_area_square_km, number_of_townships, number_of_villages, 
                number_of_neighborhoods, household_count, population_count, population_growth_rate, 
                male_population_count, female_population_count, gender_ratio, average_household_size, 
                population_density_per_square_km
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                time_name = VALUES(time_name),
                land_area_square_km = VALUES(land_area_square_km),
                number_of_townships = VALUES(number_of_townships),
                number_of_villages = VALUES(number_of_villages),
                number_of_neighborhoods = VALUES(number_of_neighborhoods),
                household_count = VALUES(household_count),
                population_count = VALUES(population_count),
                population_growth_rate = VALUES(population_growth_rate),
                male_population_count = VALUES(male_population_count),
                female_population_count = VALUES(female_population_count),
                gender_ratio = VALUES(gender_ratio),
                average_household_size = VALUES(average_household_size),
                population_density_per_square_km = VALUES(population_density_per_square_km)
            """
            cursor.executemany(upsert_query, data_to_insert)
            conn.commit()
    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        conn.close()


# 定義DAG和其默認參數
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
    'population_pipeline',
    default_args=default_args,
    description='A pipeline for crawling population and processing the data.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 13),
    catchup=False
)

t1 = PythonOperator(
    task_id='population_upload_to_S3',
    python_callable=crawl_population,
    dag=dag,
)

t2 = PythonOperator(
    task_id='fetch_population_dag_data_to_db',
    python_callable=process_population_data,
    dag=dag,
)

t1 >> t2
