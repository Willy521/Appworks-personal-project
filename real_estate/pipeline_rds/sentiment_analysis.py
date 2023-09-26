import openai
import os
from dotenv import load_dotenv
import boto3
import json
import pymysql
from decouple import config
import re
import ast


def download_file_from_s3(bucket_name, object_key, file_name):
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket_name, object_key, file_name)
        print(f"File downloaded from S3: {file_name}")
        return True
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        return False


# includes api key and calling
def call_chatGPT(prompt):

    openai.api_key = os.environ.get('OPENAI_API_KEY')
    if not openai.api_key:
        raise ValueError("No OpenAI API Key found!")

    completion = openai.ChatCompletion.create(
        model='gpt-3.5-turbo',
        messages=[
            {'role': 'user', 'content': prompt}
        ],
    )
    return completion['choices'][0]['message']['content']


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


def main():
    load_dotenv()  # AWS 環境變數

    # 定義S3的桶名，對象key和要保存的文件名
    bucket_name = 'appworks.personal.project'
    object_key = 'crawl_to_s3/housefun_popularity_data.json'
    file_name = 'housefun_popularity_data.json'

    # S3 download
    download_file_from_s3(bucket_name, object_key, file_name)

    with open(file_name, 'r', encoding='utf-8') as f:
        sentiment_data = json.load(f)
    print(json.dumps(sentiment_data, ensure_ascii=False, indent=4))

    # create sentiment_news_bing_li table
    conn = connect_to_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sentiment_news_bing_li(
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL UNIQUE,
                    date DATE NOT NULL,
                    content TEXT NOT NULL
                )
            """)
            conn.commit()
            print("sentiment_news_bing_li table has been created successfully!")
    except Exception as e:
        print(f"Failed to create table: {e}")
        conn.rollback()  # 有錯誤就不會對db有任何影響

    # 將原始新聞插入sentiment_news_bing_li
    try:
        with conn.cursor() as cursor:
            insert_data_sql = """
            INSERT IGNORE INTO sentiment_news_bing_li (title, date, content)  # 修改了表名稱
            VALUES (%s, %s, %s)
            """
            # 假設sentiment_data是一個列表，遍歷這個列表並插入每一條記錄
            for record in sentiment_data:
                cursor.execute(insert_data_sql,
                               (record["title"], record["date"], record["content"]))  # 修改了資料來源
        conn.commit()
    except Exception as e:
        print(f"Failed to insert data into MySQL: {e}")
        conn.rollback()

    # query sentiment_news_bing_li table and call openai to do sentiment analysis
    # create sentiment_result_bing_li
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sentiment_result_bing_li (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    news_id INT,
                    date DATE NOT NULL,
                    analysis_result TEXT NOT NULL,
                    FOREIGN KEY (news_id) REFERENCES sentiment_news_bing_li(id)
                )
            """)
            conn.commit()
            print("sentiment_result_bing_li table has been created successfully!")
    except Exception as e:
        print(f"Failed to create sentiment_result_bing_li table: {e}")
        conn.rollback()

    # query bing_li_news table and insert data
    # 查詢原始新聞表格
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, date, content FROM sentiment_news_bing_li")
            results = cursor.fetchall()  # 提取所有結果
            for row in results:
                news_id = row[0]
                date = row[1]
                content = row[2]

                # 呼叫 OpenAI 進行分析
                prompt = f"""
                這是一篇關於房地產的文章: '{content}'。
                根據這篇文章的內容，您認為房市目前的走勢是看漲，還是看跌？
                同時，請您為這篇文章中的房價看法打一個 0 到 10 的分數。
                0 代表強烈看跌，10 代表強烈看漲，5 代表中立。
                請以一個 list 的形式回答，第一個元素是您對房市走勢的看法（看漲或看跌），第二個元素是您給出的分數。
                """
                while True:
                    analysis_result = call_chatGPT(prompt)
                    print(analysis_result)

                    # 檢查是否有中括號
                    list_result = re.search(r'\[.*\]', analysis_result)

                    if list_result:
                        try:
                            # 使用 ast.literal_eval 安全地將字符串轉換為列表
                            extracted_list = ast.literal_eval(list_result.group(0))

                            # 確保提取到的是一個列表，並且有至少兩個元素
                            if isinstance(extracted_list, list) and len(extracted_list) >= 2:
                                score = extracted_list[1]  # 從列表中提取分數
                                print("score", score)
                                break  # 如果一切正常，就跳出循環
                            else:
                                print("找到的格式不正確，將跳過此條...")
                        except ValueError:
                            print("轉換時出錯，將跳過此條...")
                    else:
                        print("未找到期望的列表，將跳過此條...")

                # 插入結果到分析結果表格
                insert_sql = """
                        INSERT INTO sentiment_result_bing_li (news_id, date, analysis_result)
                        VALUES (%s, %s, %s)
                        """
                cursor.execute(insert_sql, (news_id, date, score))
            conn.commit()
    except Exception as e:
        print(f"Failed to query or insert data: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    main()