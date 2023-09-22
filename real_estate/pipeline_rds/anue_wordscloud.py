import json
import pymysql
from decouple import config
from dotenv import load_dotenv
import openai
import os
import re


def call_chagpt(prompt):
    completion = openai.ChatCompletion.create(
        model='gpt-3.5-turbo',
        messages=[
            {'role': 'user', 'content': prompt}
        ],
    )
    return completion['choices'][0]['message']['content']


def main():
    load_dotenv()
    openai.api_key = os.environ.get('OPENAI_API_KEY')
    if not openai.api_key:
        raise ValueError("no OpenAI API 金鑰!")

    with open('/Users/chenweiting/Desktop/AppWorks_Personal_Project/crawl_to_s3/anue_news_data.json', 'r', encoding='utf-8') as f:
        news_data = json.load(f)

    password = config('DATABASE_PASSWORD')
    password_bytes = password.encode('utf-8')

    conn = None
    try:
        conn = pymysql.connect(
            host='appworks.cwjujjrb7yo0.ap-southeast-2.rds.amazonaws.com',
            port=3306,
            user='admin',
            password=password_bytes,
            database='estate_data_hub',
            charset='utf8mb4'
        )

        if conn.open:
            print("成功連接到 MySQL 資料庫")

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
                sql = "INSERT IGNORE INTO anue_news (id, title, date, first_paragraph) VALUES (%s, %s, %s, %s)"
                cursor.execute(sql, (item['id'], item['title'], item['date'], item['first_paragraph']))

        conn.commit()

    except Exception as e:
        print(f"發生錯誤: {e}")
    finally:
        if conn:
            conn.close()

    # 第一次請 openai 將每一則新聞整理出兩句話(尚未給關鍵字)
    first_time_sentences = []
    for item in news_data:
        prompt = f"以下是一則新聞的 title 和第一段文章，幫我用繁體中文整理給我2句話{item['title']}。{item['first_paragraph']}。"
        first_time_sentences.append(call_chagpt(prompt))
    print("first_time_sentences", first_time_sentences)

    # 第二次 openai 挑出關鍵字
    prompt_1 = f"幫我從這個 list {first_time_sentences}裡面整理出 10 個有關房市的關鍵詞用list表示。我要做文字雲讓觀看者可以一眼知道最近的話題"

    while True:  # 開始一個無窮循環
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

    try:
        conn = pymysql.connect(
            host='appworks.cwjujjrb7yo0.ap-southeast-2.rds.amazonaws.com',
            port=3306,
            user='admin',
            password=password_bytes,
            database='estate_data_hub',
            charset='utf8mb4'
        )

        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS keywords_table (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    keyword VARCHAR(255)
                )
            """)
            conn.commit()

            if ten_key_words:
                sql = "INSERT IGNORE INTO keywords_table (keyword) VALUES (%s)"
                cursor.execute(sql, [ten_key_words])
                conn.commit()

    except Exception as e:
        print(f"發生錯誤: {e}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
