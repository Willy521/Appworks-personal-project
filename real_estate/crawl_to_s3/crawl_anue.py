from dotenv import load_dotenv
from bs4 import BeautifulSoup
import requests
import logging
import json
import os
from crawl_utilities import upload_to_s3, save_data_to_json_file


# crawl_news_data
def fetch_news_data(news_category_url, base_url):
    response = requests.get(news_category_url)
    if response.status_code != 200:
        logging.error("Failed to get the news_data webpage.")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')
    articles = soup.find_all('div', {'style': 'height:70px;'})

    news_data = []
    for i, article in enumerate(articles):
        title = article.find('h3')
        time = article.find('time')
        link_tag = article.find('a', {'class': '_1Zdp'})

        if title and time and link_tag:
            # print(f"{i + 1}. {title.text} ({time['datetime']})")
            href = link_tag.get('href')
            full_url = f"{base_url}{href}"
            article_response = requests.get(full_url)
            article_soup = BeautifulSoup(article_response.content, 'html.parser')
            first_div = article_soup.find('div', {'class': '_2E8y'})
            first_paragraph = first_div.find('p') if first_div else None

            if first_paragraph:
                # print(f"First Paragraph: {first_paragraph.text}")
                news_data.append({
                    'id': i + 1,
                    'title': title.text,
                    'date': time['datetime'],
                    'first_paragraph': first_paragraph.text
                })
    return news_data


def main():
    load_dotenv()
    base_url = 'https://news.cnyes.com'
    news_category_url = f'{base_url}/news/cat/tw_housenews'
    directory = 'crawl_to_s3_file'
    file_name = "anue_news_data.json"
    bucket_name = 'appworks.personal.project'

    # fetch news data
    news_data = fetch_news_data(news_category_url, base_url)
    if news_data:
        logging.info("Fetch news data successfully")
        print(json.dumps(news_data, ensure_ascii=False, indent=4))

    # save it to a json file
    save_data_to_json_file(news_data, directory, file_name)

    # Upload to S3
    json_file_path = os.path.join(directory, file_name)
    if upload_to_s3(json_file_path, bucket_name):
        logging.info("News data JSON file successfully uploaded to S3.")
    else:
        logging.error("News data failed to upload JSON file to S3.")


if __name__ == "__main__":
    logging.basicConfig(filename='crawl.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
