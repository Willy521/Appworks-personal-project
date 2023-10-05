# anue_wordcloud

import json
import pymysql
from decouple import config
from dotenv import load_dotenv
import openai
import os
import re
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


def main():
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

    conn = connect_to_db()
    try:
        with conn.cursor() as cursor:
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
    conn.close()



    # 應該還要從an news table 用id 篩選出日期前10天的新聞兒不是直接for item in news_data:
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


if __name__ == "__main__":
    main()
