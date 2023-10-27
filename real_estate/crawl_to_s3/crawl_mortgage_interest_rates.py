from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
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


def selenium_get_data(link):
    # set headless
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(link)

    # find "五代行庫平均房貸利率頁面"
    element = driver.find_element(By.ID, 'ctl00_ContentPlaceHolder1_btnItem06')
    element.click()

    element_id = 'ctl00_ContentPlaceHolder1_DropDownList3'
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, element_id))
    )

    # start year
    select_start_year = Select(element)
    select_start_year.select_by_value('083')

    # start month
    element_id = 'ctl00_ContentPlaceHolder1_DropDownList4'
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, element_id))
    )
    select_start_month = Select(element)
    select_start_month.select_by_value('01')

    # end year
    element_id = 'ctl00_ContentPlaceHolder1_DropDownList5'
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, element_id))
    )
    select_end_year = Select(element)
    all_options = select_end_year.options  # includes all options
    first_option_value = all_options[0].get_attribute('value')  # [0] option which on the top position
    select_end_year.select_by_value(first_option_value)

    # end month
    element_id = 'ctl00_ContentPlaceHolder1_DropDownList6'
    element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, element_id))
    )
    select_end_month = Select(element)
    all_options = select_end_month.options
    last_option_value = all_options[-1].get_attribute('value')  # option which on the bottom position
    select_end_month.select_by_value(last_option_value)

    # click the button
    button = driver.find_element(By.ID, 'ctl00_ContentPlaceHolder1_Button1')
    button.click()

    time.sleep(20)

    # fetch data
    data = driver.page_source  # get html source
    driver.quit()
    return data


def beautiful_soup_parse_data(data):
    soup = BeautifulSoup(data, 'html.parser')

    table = soup.find('table', class_=['table', 'table-th-center', 'table-td-center', 'table-bordered', 'table-striped',
                                       'table-responsive-sm'])
    rows = table.find_all('tr')
    data_list = []
    for row in rows:
        cells = row.find_all('td')
        if len(cells) == 2:
            time = cells[0].get_text(strip=True)
            rate = cells[1].get_text(strip=True)
            data_list.append({"time": time, "rate": rate})
    return data_list


def main():
    load_dotenv()
    directory = "crawl_to_s3_file"
    file_name = "mortgage_interest_rates.json"
    bucket_name = 'appworks.personal.project'

    # Use selenium to get data
    url = "https://pip.moi.gov.tw/V3/E/SCRE0201.aspx"
    data = selenium_get_data(url)
    # print(data)

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
