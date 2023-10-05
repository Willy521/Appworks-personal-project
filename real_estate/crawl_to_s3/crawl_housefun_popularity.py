# house_fun_popularity

import requests
from bs4 import BeautifulSoup
import json
import os
import boto3


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

def fetch_article_content(article_url):
    base_url = "https://news.housefun.com.tw"
    full_url = f"{base_url}{article_url}"

    # 掛header
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.3"
    }

    response = requests.get(full_url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to get the article at {full_url}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    content_div = soup.find('div', {'itemprop': 'articleBody'})

    if content_div:
        paragraphs = content_div.find_all('div', recursive=False)
        article_text = ' '.join(p.text.strip() for p in paragraphs)
        return article_text.strip()
    else:
        return "No content found"


def main():
    search_url = 'https://news.housefun.com.tw/news/顏炳立/?od=0&osc=1'

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.3"
    }

    response = requests.get(search_url, headers=headers)
    if response.status_code != 200:
        print("Failed to get the webpage.")
        return

    # BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    article_boxes = soup.find_all('div', class_='ct')

    articles = []
    for box in article_boxes:
        link_tag = box.find('a', title=True, class_='ga_click_trace')
        date_tag = box.find('div', class_='post-date')

        if link_tag and date_tag:
            title = link_tag.get('title', 'No Title')
            href = link_tag.get('href', 'No HREF')
            date = date_tag.text
            content = fetch_article_content(href)

            article_info = {
                'title': title,
                'date': date,
                'content': content
            }

            articles.append(article_info)

            # 如果你仍想在終端機中看到這些信息，你可以保留以下的 print 語句
            print(f"Article title: {title}")
            print(f"Article date: {date}")
            print(f"Article content: {content[:100]}...")  # 只打印文章的前100個字符作為預覽

    print("-------------------------------------------------------------------------------------------------------")

    # 存到S3

    json_file_path = 'crawl_to_s3/housefun_popularity_data.json'

    # 檢查目錄是否存在，如果不存在則創建
    directory = os.path.dirname(json_file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)

    # 寫入
    with open(json_file_path, 'w', encoding='utf-8') as f:
        json.dump(articles, f, ensure_ascii=False, indent=4)

    # Upload JSON file to S3
    if upload_file_to_s3(json_file_path, 'appworks.personal.project'):
        print("JSON file successfully uploaded to S3.")
    else:
        print("Failed to upload JSON file to S3.")

    # Pretty-print JSON data
    print(json.dumps(articles, ensure_ascii=False, indent=4))


if __name__ == "__main__":
    main()
