import json
import requests
from bs4 import BeautifulSoup
import boto3
import os


def upload_file_to_s3(file_name, bucket, object_name=None):
    s3 = boto3.client('s3')

    if object_name is None:
        object_name = file_name

    try:
        s3.upload_file(file_name, bucket, object_name)
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def fetch_cnyes_housenews():
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

    json_file_path = 'crawl_to_s3/anue_news_data.json'

    # 檢查目錄是否存在，如果不存在則創建
    directory = os.path.dirname(json_file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(json_file_path, 'w', encoding='utf-8') as f:
        json.dump(news_data, f, ensure_ascii=False, indent=4)

    # Upload JSON file to S3
    if upload_file_to_s3(json_file_path, 'appworks.personal.project'):
        print("JSON file successfully uploaded to S3.")
    else:
        print("Failed to upload JSON file to S3.")

    # Pretty-print JSON data
    print(json.dumps(news_data, ensure_ascii=False, indent=4))


if __name__ == "__main__":
    fetch_cnyes_housenews()
