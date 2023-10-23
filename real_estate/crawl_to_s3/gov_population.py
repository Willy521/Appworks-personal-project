from dotenv import load_dotenv
from datetime import datetime
import requests
import json
import os
from crawl_utilities import create_government_url, upload_to_s3, save_data_to_json_file


def main():
    load_dotenv()
    directory = 'crawl_to_s3_file'
    file_name = "population_data.json"
    bucket_name = 'appworks.personal.project'

    # get url
    api_base_url = 'https://nstatdb.dgbas.gov.tw/dgbasAll/webMain.aspx?sdmx/A130201010/1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.1.1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.M/'
    start_time = '2000-M1'
    end_time = datetime.now().strftime('%Y-M%m')  # 2023-10-20 to 2023-M10
    response = requests.get(create_government_url(api_base_url, start_time, end_time))

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