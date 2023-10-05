# s3_to_rds_gov_economic_gdp_indicator
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


if __name__ == "__main__":
    main()
