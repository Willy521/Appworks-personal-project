# s3_to_rds_gov_social_population
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
    object_key = 'crawl_to_s3_file/population_data.json'  # S3文件的名字
    file_name = 'download_from_s3_file/population_data.json'  # 本地保存的文件名

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


if __name__ == "__main__":
    main()
