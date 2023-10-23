from dotenv import load_dotenv
from datetime import datetime
import requests
import json
import os
from crawl_utilities import create_government_url, upload_to_s3, save_data_to_json_file


def quarter_format(year, month):
    if month <= 3:
        current_quarter = f"{year}-Q1"
    elif month <= 6:
        current_quarter = f"{year}-Q2"
    elif month <= 9:
        current_quarter = f"{year}-Q3"
    else:
        current_quarter = f"{year}-Q4"
    return current_quarter


def main():
    load_dotenv()
    directory = 'crawl_to_s3_file'
    file_name = "gdp_data.json"
    bucket_name = 'appworks.personal.project'

    # get url
    api_base_url = 'https://nstatdb.dgbas.gov.tw/dgbasAll/webMain.aspx?sdmx/A018101010/1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.1.1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.Ｑ/'
    start_time = '2000-Q1'
    month = datetime.now().month
    year = datetime.now().year
    response = requests.get(create_government_url(api_base_url, start_time, quarter_format(year, month)))

    # upload JSON to S3
    if response.status_code == 200:
        try:
            data = json.loads(response.text)
            print('API Response:', json.dumps(data, indent=4, ensure_ascii=False))
            save_data_to_json_file(data, directory, file_name)

            json_file_path = os.path.join(directory, file_name)
            if upload_to_s3(json_file_path, bucket_name):
                print(f"{file_name} JSON file successfully uploaded to S3.")
            else:
                print(f"{file_name} Failed to upload JSON file to S3.")
        except json.JSONDecodeError:
            print("Received data isn't a valid JSON.")
    else:
        print(f"Failed to get data: {response.status_code}")


if __name__ == "__main__":
    main()
