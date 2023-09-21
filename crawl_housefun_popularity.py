import requests
from bs4 import BeautifulSoup


def fetch_article_content(article_url):
    base_url = "https://news.housefun.com.tw"
    full_url = f"{base_url}{article_url}"

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


def fetch_articles_info():
    search_url = 'https://news.housefun.com.tw/news/顏炳立/?od=0&osc=1'

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.3"
    }

    response = requests.get(search_url, headers=headers)
    if response.status_code != 200:
        print("Failed to get the webpage.")
        return

    soup = BeautifulSoup(response.content, 'html.parser')
    article_boxes = soup.find_all('div', class_='ct')

    for box in article_boxes:
        link_tag = box.find('a', title=True, class_='ga_click_trace')
        date_tag = box.find('div', class_='post-date')

        if link_tag and date_tag:
            title = link_tag.get('title', 'No Title')
            href = link_tag.get('href', 'No HREF')
            date = date_tag.text

            print(f"Article title: {title}")
            print(f"Article link: {href}")
            print(f"Article date: {date}")

            content = fetch_article_content(href)
            print(f"Article content: {content}...")  # 只打印文章的前100個字符作為預覽


if __name__ == "__main__":
    fetch_articles_info()
