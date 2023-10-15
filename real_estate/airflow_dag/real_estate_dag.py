from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import zipfile
import time
import pymysql
from decouple import config
from dotenv import load_dotenv
import os
import re
import boto3
import csv
import pandas as pd


S3_BUCKET = os.getenv('S3_BUCKET', 'appworks.personal.project')
MAIN_FOLDER = 'real_estate_price'


def get_real_estate_url(year, season):  # which year, season
    # 調整
    if year > 1000:
        year -= 1911
    url = "https://plvr.land.moi.gov.tw//DownloadSeason?season=" + str(year) + "S" + str(
        season) + "&type=zip&fileName=lvr_landcsv.zip"
    return url


def get_price_information(url, year, season):
    print("Now is: ", year, season)
    print(url)
    res = requests.get(url)
    print("Status Code:", res.status_code)  # 看有沒有response
    print("Content Length:", len(res.content))  # 看長度多少

    # 代表還沒有數據直接return
    if res.status_code != 200 or len(res.content) < 5000:
        print(f"No data available for {year} Season {season}. Stopping.")
        return
    # 有數據就創建新目錄
    else:
        fname = str(year) + str(season) + '.zip'
        main_folder = 'real_estate_price'
        sub_folder = f"{main_folder}/unzipped_{year}_{season}"
        # 確認有數據後創建目錄
        if not os.path.isdir(main_folder):
            os.mkdir(main_folder)
        if not os.path.isdir(sub_folder):
            os.mkdir(sub_folder)

        # 解壓縮
        zip_path = os.path.join(main_folder, fname)
        with open(zip_path, 'wb') as file:
            file.write(res.content)
        # 上傳 zip 文件到 S3
        upload_to_s3(zip_path, S3_BUCKET, object_name=f"{MAIN_FOLDER}/{fname}")

        # 解壓縮 zip 文件並將解壓縮的檔案也上傳到 S3
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                print("Extracting files...")
                zip_ref.extractall(sub_folder)
                for file_info in zip_ref.infolist():
                    extracted_file_path = os.path.join(sub_folder, file_info.filename)
                    print(f"Processing {extracted_file_path}...")

                    # S3 中保持相同的目錄結構
                    s3_object_name = f"{sub_folder}/{file_info.filename}"
                    print(f"Uploading {extracted_file_path} to S3 as {s3_object_name}...")
                    success = upload_to_s3(extracted_file_path, S3_BUCKET, object_name=s3_object_name)
                    if not success:
                        print(f"Failed to upload {extracted_file_path} to S3.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")


def upload_to_s3(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name

    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, object_name)
    except Exception as e:
        print("S3 Upload Error:", e)
        return False
    return True


def transform_local_file_name(file_name):
    if 'unzipped_' in file_name:
        parts = file_name.split('_')
        year = int(parts[1]) + 1911
        return f"{MAIN_FOLDER}/unzipped_{year}_{parts[2]}"
    return file_name


def list_existing_files_local(folder):
    if os.path.exists(folder):  # 檢查文件夾是否存在
        file_names = os.listdir(folder)  # 獲取文件夾中所有文件和子文件夾的名稱
        transformed_names = set(transform_local_file_name(name) for name in file_names if name.startswith('unzipped_'))  # 轉換名稱並過濾
        return transformed_names  # 返回轉換後的名稱集合
    else:
        return set()  # 如果文件夾不存在，返回一個空集合


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


def download_file_from_s3(bucket_name, object_key):
    s3 = boto3.client('s3')

    # 將 "real_estate_price/" 從 object_key 中移除，並使用其餘部分作為文件名
    local_file_name = object_key.replace('real_estate_price/', '')
    local_file_path = os.path.join('real_estate_price', local_file_name)

    # 確保目錄存在
    if not os.path.exists(os.path.dirname(local_file_path)):
        os.makedirs(os.path.dirname(local_file_path))

    try:
        s3.download_file(bucket_name, object_key, local_file_path)
        print(f"File downloaded from S3 to: {local_file_path}")
        return local_file_path
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        return None


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
                id INT AUTO_INCREMENT,
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
                serial_number VARCHAR(255) NOT NULL UNIQUE,
                build_case VARCHAR(255),
                buildings_and_number VARCHAR(255),
                PRIMARY KEY(id)
            )
            """
            cursor.execute(sql)
            conn.commit()
            print("Table created successfully")
    except Exception as e:
        print(f"Error creating table: {e}")


def insert_data_from_csv(conn, file_name):
    try:
        data = pd.read_csv(file_name, encoding='utf-8', on_bad_lines='skip')
        # Replace NaN values with empty string or some other value
        data = data.fillna("NAN")

        data = data.drop(data.index[0], axis=0)  # 刪除第一行 標題

        # 檢查文件名中是否包含"112_3"（代表第3季），如果是則刪除最後一個column
        print("Processing file: ", file_name)
        if "112_3" in file_name:
            print(data.head())
            data = data.drop(data.columns[-1], axis=1)  # 刪除最後一個column
            print(data.head())

        with conn.cursor() as cursor:
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

            # 將pandas dataframe的每一行轉化為tuple
            data_to_insert = [tuple(row) for index, row in data.iterrows()]

            # Use executemany to insert all data at once
            cursor.executemany(sql, data_to_insert)

        conn.commit()
        print(f"{file_name} Data inserted successfully")
    except Exception as e:
        print(f"{file_name} Error inserting data: {e}")







def crawl_real_estate():
    load_dotenv()  # S3環境變數

    # 計算本地應該要有哪些檔案
    current_year = int(time.strftime('%Y'))
    current_season = (int(time.strftime('%m')) - 1) // 3 + 1

    expected_files = set()
    for year in range(current_year, 2019, -1):  #
        for season in range(1, 5):  # 4 seasons in a year
            if year == current_year and season > current_season:
                continue
            expected_files.add(f"{MAIN_FOLDER}/unzipped_{year}_{season}")

    # 檢查本地缺少什麼檔案
    existing_files_local = list_existing_files_local(MAIN_FOLDER)
    print('本地應該有檔案: ', expected_files)
    print('本地已經有檔案: ', existing_files_local)

    # 計算並輸出缺少的檔案
    missing_files = expected_files - existing_files_local
    print('missing_files: ', missing_files)

    # 為缺少的檔案製作url和下載和解壓縮數據
    for missing_file in missing_files:
        # 從文件名解析年和季度
        parts = missing_file.split('_')
        year = int(parts[-2])  # 從最後一個下划線前取值，獲取年份
        year -= 1911
        season = int(parts[-1])  # 從最後一個下划線後取值，獲取季度
        url = get_real_estate_url(year, season)  # 已轉成要下載的url
        # print(url)
        get_price_information(url, year, season)


def process_real_estate_data():
    load_dotenv()  # 載入環境變數

    # 定義S3的桶名，對象key和要保存的文件名
    bucket_name = 'appworks.personal.project'
    # file_name = 'real_estate_price/last_download.csv'  # 下載後在本地保存的文件

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
        downloaded_file_path = download_file_from_s3(bucket_name, object_key)
        if downloaded_file_path:
            # 如果文件下載成功，插入到DB
            insert_data_from_csv(conn, downloaded_file_path)
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
    'real_estate_pipeline',  # DAG 的唯一識別碼
    default_args=default_args,  # 上面定義的默認參數
    description='A pipeline for real estate price and processing the data.',  # DAG 的描述
    schedule_interval=timedelta(days=1),  # DAG的執行間隔。這裡設定為每天一次
    start_date=datetime(2023, 10, 14),  # DAG 的開始日期
    catchup=False  # 若為 True，則當 DAG 啟動時，將會執行從 start_date 到當前日期之間的所有排程。若為 False，則只會執行最新的排程。
)

t1 = PythonOperator(
    task_id='real_estate_upload_to_S3',
    python_callable=crawl_real_estate,
    dag=dag,  # 指定該任務屬於哪個 DAG。
)

t2 = PythonOperator(
    task_id='process_real_estate_data',
    python_callable=process_real_estate_data,
    dag=dag,
)

t1 >> t2
