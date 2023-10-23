from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium import webdriver
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import time
import json
import os
from crawl_utilities import upload_to_s3, save_data_to_json_file


def selenium_select_data(link):
    # headless
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(link)

    # start time
    select_start = Select(driver.find_element(By.ID, 'ctl00_ContentPlaceHolder1_DropDownList1'))
    select_start.select_by_value('20001')

    # end time
    select_end = Select(driver.find_element(By.ID, 'ctl00_ContentPlaceHolder1_DropDownList2'))
    all_options = select_end.options  # includes all options
    first_option_value = all_options[0].get_attribute('value')  # [0] option which on the top position
    select_end.select_by_value(first_option_value)

    # select city
    checkbox_ids = [
        'ctl00_ContentPlaceHolder1_CheckBoxList3_1',  # "台北市"
        'ctl00_ContentPlaceHolder1_CheckBoxList3_2',  # "新北市"
        'ctl00_ContentPlaceHolder1_CheckBoxList3_4',  # "桃園市"
        'ctl00_ContentPlaceHolder1_CheckBoxList3_5',  # "新竹縣市"
        'ctl00_ContentPlaceHolder1_CheckBoxList3_6',  # "台中市"
        'ctl00_ContentPlaceHolder1_CheckBoxList3_7',  # "台南市"
        'ctl00_ContentPlaceHolder1_CheckBoxList3_8'  # "高雄市"
    ]

    for checkbox_id in checkbox_ids:
        checkbox = driver.find_element(By.ID, checkbox_id)
        if not checkbox.is_selected():
            checkbox.click()

    # hit the button
    button = driver.find_element(By.ID, 'ctl00_ContentPlaceHolder1_Button1')
    button.click()

    time.sleep(5)

    data = driver.page_source  # get html source
    driver.quit()
    return data


def beautiful_soup_parse_data(data):
    soup = BeautifulSoup(data, 'html.parser')
    table = soup.find('table', class_='table')
    data_list = []
    for row in table.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) > 1:  # exclude the first cell
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
    load_dotenv()
    url = "https://pip.moi.gov.tw/V3/E/SCRE0201.aspx"
    directory = "crawl_to_s3_file"
    file_name = "house_cathay_index.json"
    bucket_name = 'appworks.personal.project'

    # use selenium
    data = selenium_select_data(url)

    # beautiful soup parse information
    data_list = beautiful_soup_parse_data(data)
    json_data = json.dumps(data_list, ensure_ascii=False, indent=4)
    print(json_data)

    # save it to a json file
    save_data_to_json_file(json_data, directory, file_name)

    # Upload to S3
    json_file_path = os.path.join(directory, file_name)
    if upload_to_s3(json_file_path, bucket_name):
        print("JSON file successfully uploaded to S3.")
    else:
        print("Failed to upload JSON file to S3.")


if __name__ == "__main__":
    main()
