# 國泰房價指數(內政部不動產資訊平台)

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
import time
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import json
import boto3
from dotenv import load_dotenv
import os


# 上傳到S3
def upload_file_to_s3(file_name, bucket):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, file_name)  # 本地的文件路徑跟S3設為一樣
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def selenium_get_data(link):
    # 創建一個Options對象並設headless 才能在EC2上跑
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')

    # 創建一個新的瀏覽器實例，並傳入上面創建的Options對象
    driver = webdriver.Chrome(options=chrome_options)

    driver.get(link)

    # find 時間篩選器
    select_start = Select(driver.find_element(By.ID, 'ctl00_ContentPlaceHolder1_DropDownList1'))
    select_start.select_by_value('20001')

    # selenium 取下拉選單第一個選項
    select_end = Select(driver.find_element(By.ID, 'ctl00_ContentPlaceHolder1_DropDownList2'))
    all_options = select_end.options  # 是一個包含了所有選項的列表
    last_option_value = all_options[0].get_attribute('value')  # [0] 是選下拉式選單最上面的，通常才是最新的數據
    select_end.select_by_value(last_option_value)

    # find 縣市篩選器
    checkbox_ids = [
        'ctl00_ContentPlaceHolder1_CheckBoxList3_1',  # 台北市
        'ctl00_ContentPlaceHolder1_CheckBoxList3_2',  # 新北市
        'ctl00_ContentPlaceHolder1_CheckBoxList3_4',  # 桃園市
        'ctl00_ContentPlaceHolder1_CheckBoxList3_5',  # 新竹縣市
        'ctl00_ContentPlaceHolder1_CheckBoxList3_6',  # 台中市
        'ctl00_ContentPlaceHolder1_CheckBoxList3_7',  # 台南市
        'ctl00_ContentPlaceHolder1_CheckBoxList3_8'  # 高雄市
    ]

    for checkbox_id in checkbox_ids:
        checkbox = driver.find_element(By.ID, checkbox_id)
        if not checkbox.is_selected():
            checkbox.click()

    # 點擊查詢按鈕
    button = driver.find_element(By.ID, 'ctl00_ContentPlaceHolder1_Button1')
    button.click()

    time.sleep(5)

    # 獲取數據
    data = driver.page_source  # 這會獲取當前頁面的HTML源碼
    driver.quit()
    return data


def beautiful_soup_parse_data(data):
    soup = BeautifulSoup(data, 'html.parser')
    table = soup.find('table', class_='table')
    data_list = []
    for row in table.find_all('tr'):
        # 取得該行的所有單元格
        cells = row.find_all('td')
        if len(cells) > 1:  # 排除表頭
            # 構建一個字典，每個單元格的文本作為值，相應的標頭作為鍵
            data = {
                '年度季別': cells[0].get_text(strip=True),
                '全國': float(cells[1].get_text(strip=True)),
                '台北市': float(cells[2].get_text(strip=True)),
                '新北市': float(cells[3].get_text(strip=True)),
                '桃園市': float(cells[4].get_text(strip=True)),
                '新竹縣市': float(cells[5].get_text(strip=True)),
                '台中市': float(cells[6].get_text(strip=True)),
                '台南市': float(cells[7].get_text(strip=True)),
                '高雄市': float(cells[8].get_text(strip=True)),
            }
            data_list.append(data)
    return data_list


def main():
    load_dotenv()  # 環境變數

    # Use selenium to get data
    url = "https://pip.moi.gov.tw/V3/E/SCRE0201.aspx"
    data = selenium_get_data(url)

    # beautiful soup parse information
    data_list = beautiful_soup_parse_data(data)

    # 轉換為JSON格式
    json_data = json.dumps(data_list, ensure_ascii=False, indent=4)
    print(json_data)

    # Save as JSON file
    # 在本地創建一個資料夾，將JSON file 存入資料夾並上傳到S3
    directory = "crawl_to_s3_file"
    if not os.path.exists(directory):
        os.makedirs(directory)
    json_file_path = "crawl_to_s3_file/house_cathay_index.json"

    with open(json_file_path, 'w', encoding='utf-8') as f:
        json.dump(data_list, f, ensure_ascii=False, indent=4)

    # Upload to S3
    bucket_name = 'appworks.personal.project'  # Replace with your bucket name
    if upload_file_to_s3(json_file_path, bucket_name):
        print("JSON file successfully uploaded to S3.")
    else:
        print("Failed to upload JSON file to S3.")


if __name__ == "__main__":
    main()
