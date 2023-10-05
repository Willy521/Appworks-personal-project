# 人口
import requests
import json
import boto3
from dotenv import load_dotenv
from datetime import datetime
import os


def create_url(start_time, last_date_queried):
    api_base_url = 'https://nstatdb.dgbas.gov.tw/dgbasAll/webMain.aspx?sdmx/A130201010/1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.1.1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.M/'
    updated_url = f"{api_base_url}&startTime={start_time}&endTime={last_date_queried}"
    return updated_url


# 上傳到S3
def upload_file_to_s3(file_name, bucket):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, file_name)  # 本地的文件路徑跟S3設為一樣
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def main():
    load_dotenv()  # S3環境變數

    start_time = '2000-M1' # Set start_time
    now = datetime.now()  # Get current year and month for end_time
    end_time = now.strftime('%Y-M%m')  # Format it to 'YYYY-MM'
    # print('end_time', end_time)
    response = requests.get(create_url(start_time, end_time))

    if response.status_code == 200:
        try:
            data = json.loads(response.text)
            print('API Response:')
            print(json.dumps(data, indent=4, ensure_ascii=False))  # Pretty print the output

            # Save as JSON file
            # 在本地創建一個資料夾，將JSON file 存入資料夾並上傳到S3
            directory = "crawl_to_s3_file"
            if not os.path.exists(directory):
                os.makedirs(directory)
            json_file_path = "crawl_to_s3_file/population_data.json"  # 存到S3的crawl_to_s3資料夾

            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            # Upload to S3
            bucket_name = 'appworks.personal.project'  # Replace with your bucket name
            if upload_file_to_s3(json_file_path, bucket_name):
                print("JSON file successfully uploaded to S3.")
            else:
                print("Failed to upload JSON file to S3.")

        except json.JSONDecodeError:
            print("Received data isn't a valid JSON. Printing raw data:")
            print(response.text)
    else:
        print(f"Failed to get data: {response.status_code}")
        return None


if __name__ == "__main__":
    main()