# craw_anue.py

import json
import requests
from bs4 import BeautifulSoup
import boto3
import os
from dotenv import load_dotenv


# 上傳到S3
def upload_file_to_s3(file_name, bucket):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, file_name)  # 本地的文件路徑跟S3設為一樣
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def main():
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


if __name__ == "__main__":
    main()
