
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from bs4 import BeautifulSoup
import json
import pymysql
from decouple import config
from dotenv import load_dotenv
import openai
import os
import re
import boto3


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


# api 金鑰
def get_api_key():
    load_dotenv()
    api_key = os.environ.get('OPENAI_API_KEY')
    if not api_key:
        raise ValueError("API key are not be set.")  # 如果沒有API就停止接下來的動作
    return api_key


# call chatGPT
def call_chagpt(prompt):
    completion = openai.ChatCompletion.create(
        model='gpt-3.5-turbo',
        messages=[
            {'role': 'user', 'content': prompt}
        ],
    )
    return completion['choices'][0]['message']['content']


def download_file_from_s3(bucket_name, object_key, file_name):
    s3 = boto3.client('s3')

    try:
        s3.download_file(bucket_name, object_key, file_name)
        print(f"File downloaded from S3: {file_name}")
        return True
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        return False


def crawl_anue():
    # craw_anue.py
    # 上傳到S3
    load_dotenv()
    base_url = 'https://news.cnyes.com'
    url = f'{base_url}/news/cat/tw_housenews'
    response = requests.get(url)

    if response.status_code != 200:
        print("Failed to get the webpage.")
        return

    soup = BeautifulSoup(response.content, 'html.parser')
    articles = soup.find_all('div', {'style': 'height:70px;'})

    news_data = []
    for i, article in enumerate(articles):
        title = article.find('h3')
        time = article.find('time')
        link_tag = article.find('a', {'class': '_1Zdp'})

        if title and time and link_tag:
            print(f"{i + 1}. {title.text} ({time['datetime']})")

            href = link_tag.get('href')
            full_url = f"{base_url}{href}"
            article_response = requests.get(full_url)
            article_soup = BeautifulSoup(article_response.content, 'html.parser')
            first_div = article_soup.find('div', {'class': '_2E8y'})
            first_paragraph = first_div.find('p') if first_div else None

            if first_paragraph:
                print(f"First Paragraph: {first_paragraph.text}")

                news_data.append({
                    'id': i + 1,
                    'title': title.text,
                    'date': time['datetime'],
                    'first_paragraph': first_paragraph.text
                })

    print("-------------------------------------------------------------------------------------------------------")

    print(json.dumps(news_data, ensure_ascii=False, indent=4))

    # Save as JSON file
    # 在本地創建一個資料夾，將JSON file 存入資料夾並上傳到S3
    directory = 'crawl_to_s3_file'
    if not os.path.exists(directory):
        os.makedirs(directory)
    # 在資料夾內創建JSON
    json_file_path = "crawl_to_s3_file/anue_news_data.json"
    with open(json_file_path, 'w', encoding='utf-8') as f:
        json.dump(news_data, f, ensure_ascii=False, indent=4)

    # Upload to S3
    bucket_name = 'appworks.personal.project'  # Replace with your bucket name
    if upload_file_to_s3(json_file_path, bucket_name):
        print("JSON file successfully uploaded to S3.")
    else:
        print("Failed to upload JSON file to S3.")


def process_anue_data():
    openai.api_key = get_api_key()

    # 定義S3的桶名，對象key和要保存的文件名
    bucket_name = 'appworks.personal.project'
    object_key = 'crawl_to_s3_file/anue_news_data.json'  # S3文件的名字
    file_name = "download_from_s3_file/anue_news_data.json"  # 本地保存的文件名

    # S3 download
    # 在本地創建一個資料夾，將JSON file 存入本地資料夾
    directory = "download_from_s3_file"
    if not os.path.exists(directory):
        os.makedirs(directory)

    download_file_from_s3(bucket_name, object_key, file_name)
    with open(file_name, 'r', encoding='utf-8') as f:
        news_data = json.load(f)
        formatted_json = json.dumps(news_data, ensure_ascii=False, indent=4)
        print(formatted_json)

    conn = connect_to_db()
    try:
        with conn.cursor() as cursor:
            # 刪除表格
            cursor.execute("DROP TABLE IF EXISTS anue_news")
            cursor.execute("""
                    CREATE TABLE IF NOT EXISTS anue_news(
                        id INT PRIMARY KEY,
                        title VARCHAR(255),
                        date DATETIME,
                        first_paragraph TEXT
                    )
                """)
            conn.commit()
            for item in news_data:
                # 檢查標題是否含有〈房產〉
                if '〈房產〉' in item['title']:
                    # 如果含有〈房產〉，則執行插入操作
                    sql = "INSERT IGNORE INTO anue_news (id, title, date, first_paragraph) VALUES (%s, %s, %s, %s)"
                    cursor.execute(sql, (item['id'], item['title'], item['date'], item['first_paragraph']))
            print("have inserted")
        conn.commit()
    except Exception as e:
        print(f"error: {e}")
    finally:
        if conn:
            conn.close()

    # 第一次請 openai 將每一則新聞整理出兩句話(尚未給關鍵字)
    first_time_sentences = []
    for item in news_data:
        prompt = f"以下是一則新聞的 title 和第一段文章，幫我用繁體中文整理給我1句話{item['title']}。{item['first_paragraph']}。"
        first_time_sentences.append(call_chagpt(prompt))
    print("first_time_sentences", first_time_sentences)

    # 第二次 openai 挑出關鍵句子
    prompt_1 = f"幫我從這個 list {first_time_sentences} 仔細審查每個句子，濃縮成10個具有代表性描述的名詞或短句(每一句10字以內)，請把10個結果裝在一個list的形式給我。我要做文字雲讓觀看者可以一眼知道最近的話題"

    # 開始一個無窮循環
    while True:
        keywords_list = call_chagpt(prompt_1)
        print("keywords_list", keywords_list)

        # 使用正則表達式找到所有關鍵字
        ten_key_words = re.search(r'\[(.*?)\]', keywords_list)
        print("ten_key_words", ten_key_words)

        # 檢查是否獲得了期望的列表
        if ten_key_words:
            ten_key_words = ten_key_words.group(0)
            print("ten_key_words", ten_key_words)
            break  # 如果找到了，則跳出循環
        else:
            print("未找到期望的列表，重新請求OpenAI...")

    # 將結果插入到DB
    conn = connect_to_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                    CREATE TABLE IF NOT EXISTS keywords_table (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        keyword VARCHAR(255)
                    )
                """)
            sql = "INSERT IGNORE INTO keywords_table (keyword) VALUES (%s)"
            cursor.execute(sql, [ten_key_words])
            conn.commit()
    except Exception as e:
        print(f"發生錯誤: {e}")
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
    'anue_pipeline',  # DAG 的唯一識別碼
    default_args=default_args,  # 上面定義的默認參數
    description='A pipeline for crawling Anue news and processing the data.',  # DAG 的描述
    schedule_interval=timedelta(days=1),  # DAG的執行間隔。這裡設定為每天一次
    start_date=datetime(2023, 10, 13),  # DAG 的開始日期
    catchup=False  # 若為 True，則當 DAG 啟動時，將會執行從 start_date 到當前日期之間的所有排程。若為 False，則只會執行最新的排程。
)

t1 = PythonOperator(
    task_id='crawl_anue',
    python_callable=crawl_anue,
    dag=dag,  # 指定該任務屬於哪個 DAG。
)

t2 = PythonOperator(
    task_id='process_anue_data',
    python_callable=process_anue_data,
    dag=dag,
)

t1 >> t2
