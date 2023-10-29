from decouple import config
import openai
import re
from pipeline_utilities import download_and_print_json_from_s3
from utilities.utils import connect_to_db


def get_api_key():
    api_key = config('DATABASE_PASSWORD')
    if not api_key:
        raise ValueError("API key are not be set.")
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


def store_anue_news(news_data):
    conn = connect_to_db('anue_news')
    if not conn:
        print("Failed to establish a database connection.")
        return
    try:
        with conn.cursor() as cursor:
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
                if '〈房產〉' in item['title']:
                    sql = "INSERT IGNORE INTO anue_news (id, title, date, first_paragraph) VALUES (%s, %s, %s, %s)"
                    cursor.execute(sql, (item['id'], item['title'], item['date'], item['first_paragraph']))
            print("have inserted")
        conn.commit()
    except Exception as e:
        print(f"error: {e}")
    finally:
        if conn:
            conn.close()


def get_keywords_from_anue_news(news_data):
    """Processes news data and returns a list of representative keywords using OpenAI."""

    # Firstly, request OpenAI to organize two sentences
    first_time_sentences = []
    for item in news_data:
        prompt = f"以下是一則新聞的 title 和第一段文章，幫我用繁體中文整理給我1句話{item['title']}。{item['first_paragraph']}。"
        first_time_sentences.append(call_chagpt(prompt))
    print("first_time_sentences", first_time_sentences)

    # Secondly, request OpenAI to respond with a key sentence or keyword
    prompt_1 = f"幫我從這個 list {first_time_sentences} 仔細審查每個句子，濃縮成10個具有代表性描述的名詞或短句(每一句10字以內)，請把10個結果裝在一個list的形式給我。我要做文字雲讓觀看者可以一眼知道最近的話題"

    while True:
        keywords_list = call_chagpt(prompt_1)
        print("keywords_list", keywords_list)

        # Regular expression
        ten_key_words_match = re.search(r'\[(.*?)\]', keywords_list)
        print("ten_key_words", ten_key_words_match)

        if ten_key_words_match:
            ten_key_words = ten_key_words_match.group(0)
            print("ten_key_words", ten_key_words)
            return ten_key_words
        else:
            print("Expected list not found, making a new request to OpenAI...")


def store_keywords(ten_key_words):
    conn = connect_to_db('keywords_table')
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


def main():
    openai.api_key = get_api_key()

    directory = "download_from_s3_file"  # create directory
    bucket_name = 'appworks.personal.project'
    object_key = 'crawl_to_s3_file/anue_news_data.json'  # download from
    file_name = "download_from_s3_file/anue_news_data.json"  # local path

    news_data = download_and_print_json_from_s3(directory, bucket_name, object_key, file_name)

    # store anue news
    store_anue_news(news_data)

    # generate keywords
    ten_key_words = get_keywords_from_anue_news(news_data)

    # store keywords
    store_keywords(ten_key_words)


if __name__ == "__main__":
    main()
