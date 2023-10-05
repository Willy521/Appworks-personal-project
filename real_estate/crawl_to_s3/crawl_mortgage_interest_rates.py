# 五代行庫平均房貸利率(內政部不動產資訊平台)


from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
import time
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import json
import boto3
from dotenv import load_dotenv
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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

    # find 五代行庫平均房貸利率頁面
    element = driver.find_element(By.ID, 'ctl00_ContentPlaceHolder1_btnItem06')
    element.click()

    # find 時間
    # 查詢開始的年份
    element_id = 'ctl00_ContentPlaceHolder1_DropDownList3'

    # 等待特定ID的元素出現在頁面上
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, element_id))
    )
    select_start_year = Select(element)
    select_start_year.select_by_value('083')

    # 查詢開始的月份
    element_id = 'ctl00_ContentPlaceHolder1_DropDownList4'
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, element_id))
    )
    select_start_month = Select(element)
    select_start_month.select_by_value('01')


    # 查詢結束的年份
    element_id = 'ctl00_ContentPlaceHolder1_DropDownList5'
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, element_id))
    )
    select_end_year = Select(element)
    all_options = select_end_year.options
    last_option_value = all_options[0].get_attribute('value')  # 使用第一個選項的value屬性來選擇
    select_end_year.select_by_value(last_option_value)

    # 查詢結束的月份
    element_id = 'ctl00_ContentPlaceHolder1_DropDownList6'
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, element_id))
    )
    select_end_month = Select(element)
    all_options = select_end_month.options
    last_option_value = all_options[-1].get_attribute('value')  # 使用最後一個選項的value屬性來選擇
    select_end_month.select_by_value(last_option_value)

    # 點擊查詢按鈕
    button = driver.find_element(By.ID, 'ctl00_ContentPlaceHolder1_Button1')
    button.click()

    time.sleep(20)

    # 獲取數據
    data = driver.page_source  # 這會獲取當前頁面的HTML源碼
    driver.quit()
    return data


def beautiful_soup_parse_data(data):
    soup = BeautifulSoup(data, 'html.parser')

    # 定位表格
    table = soup.find('table', class_=['table', 'table-th-center', 'table-td-center', 'table-bordered', 'table-striped',
                                       'table-responsive-sm'])

    # 提取表格數據
    rows = table.find_all('tr')

    # 初始化一個列表來保存所有的數據
    data_list = []

    # 遍歷表格中的每一行
    for row in table.find_all('tr'):
        # 在每一行中找到所有的單元格
        cells = row.find_all('td')
        # 如果這一行有兩個單元格（即，時間和利率），那麼提取它們並添加到列表中
        if len(cells) == 2:
            time = cells[0].get_text(strip=True)
            rate = cells[1].get_text(strip=True)
            data_list.append({"time": time, "rate": rate})
    return data_list


def main():
    load_dotenv()  # 環境變數

    # Use selenium to get data
    url = "https://pip.moi.gov.tw/V3/E/SCRE0201.aspx"
    data = selenium_get_data(url)
    # print(data)

    # beautiful soup parse information
    data_list = beautiful_soup_parse_data(data)
    # print(data_list)

    # 轉換為JSON格式
    json_data = json.dumps(data_list, ensure_ascii=False, indent=4)
    print(json_data)

    # Save as JSON file
    # 在本地創建一個資料夾，將JSON file 存入資料夾並上傳到S3
    directory = "crawl_to_s3_file"
    if not os.path.exists(directory):
        os.makedirs(directory)
    json_file_path = "crawl_to_s3_file/mortgage_interest_rates.json"

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
