# real_estate_price

import requests
import os
import zipfile
import time
import boto3
from dotenv import load_dotenv

S3_BUCKET = os.getenv('S3_BUCKET', 'appworks.personal.project')
MAIN_FOLDER = 'real_estate_price'


def get_real_estate_url(year, season):  # which year, season
    # 調整
    if year > 1000:
        year -= 1911
    url = "https://plvr.land.moi.gov.tw//DownloadSeason?season=" + str(year) + "S" + str(
        season) + "&type=zip&fileName=lvr_landcsv.zip"
    return url


def get_price_information(url, year, season):
    print("Now is: ", year, season)
    print(url)
    res = requests.get(url)
    print("Status Code:", res.status_code)  # 看有沒有response
    print("Content Length:", len(res.content))  # 看長度多少

    # 代表還沒有數據直接return
    if res.status_code != 200 or len(res.content) < 5000:
        print(f"No data available for {year} Season {season}. Stopping.")
        return
    # 有數據就創建新目錄
    else:
        fname = str(year) + str(season) + '.zip'
        main_folder = 'real_estate_price'
        sub_folder = f"{main_folder}/unzipped_{year}_{season}"
        # 確認有數據後創建目錄
        if not os.path.isdir(main_folder):
            os.mkdir(main_folder)
        if not os.path.isdir(sub_folder):
            os.mkdir(sub_folder)

        # 解壓縮
        zip_path = os.path.join(main_folder, fname)
        with open(zip_path, 'wb') as file:
            file.write(res.content)
        # 上傳 zip 文件到 S3
        upload_to_s3(zip_path, S3_BUCKET, object_name=f"{MAIN_FOLDER}/{fname}")

        # 解壓縮 zip 文件並將解壓縮的檔案也上傳到 S3
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                print("Extracting files...")
                zip_ref.extractall(sub_folder)
                for file_info in zip_ref.infolist():
                    extracted_file_path = os.path.join(sub_folder, file_info.filename)
                    print(f"Processing {extracted_file_path}...")

                    # S3 中保持相同的目錄結構
                    s3_object_name = f"{sub_folder}/{file_info.filename}"
                    print(f"Uploading {extracted_file_path} to S3 as {s3_object_name}...")
                    success = upload_to_s3(extracted_file_path, S3_BUCKET, object_name=s3_object_name)
                    if not success:
                        print(f"Failed to upload {extracted_file_path} to S3.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")


def upload_to_s3(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name

    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, object_name)
    except Exception as e:
        print("S3 Upload Error:", e)
        return False
    return True


def transform_local_file_name(file_name):
    if 'unzipped_' in file_name:
        parts = file_name.split('_')
        year = int(parts[1]) + 1911
        return f"{MAIN_FOLDER}/unzipped_{year}_{parts[2]}"
    return file_name


def list_existing_files_local(folder):
    if os.path.exists(folder):  # 檢查文件夾是否存在
        file_names = os.listdir(folder)  # 獲取文件夾中所有文件和子文件夾的名稱
        transformed_names = set(transform_local_file_name(name) for name in file_names if name.startswith('unzipped_'))  # 轉換名稱並過濾
        return transformed_names  # 返回轉換後的名稱集合
    else:
        return set()  # 如果文件夾不存在，返回一個空集合


def main():
    load_dotenv()  # S3環境變數

    # 計算本地應該要有哪些檔案
    current_year = int(time.strftime('%Y'))
    current_season = (int(time.strftime('%m')) - 1) // 3 + 1

    expected_files = set()
    for year in range(current_year, 2019, -1):  #
        for season in range(1, 5):  # 4 seasons in a year
            if year == current_year and season > current_season:
                continue
            expected_files.add(f"{MAIN_FOLDER}/unzipped_{year}_{season}")

    # 檢查本地缺少什麼檔案
    existing_files_local = list_existing_files_local(MAIN_FOLDER)
    print('本地應該有檔案: ', expected_files)
    print('本地已經有檔案: ', existing_files_local)

    # 計算並輸出缺少的檔案
    missing_files = expected_files - existing_files_local
    print('missing_files: ', missing_files)

    # 為缺少的檔案製作url和下載和解壓縮數據
    for missing_file in missing_files:
        # 從文件名解析年和季度
        parts = missing_file.split('_')
        year = int(parts[-2])  # 從最後一個下划線前取值，獲取年份
        year -= 1911
        season = int(parts[-1])  # 從最後一個下划線後取值，獲取季度
        url = get_real_estate_url(year, season)  # 已轉成要下載的url
        # print(url)
        get_price_information(url, year, season)


if __name__ == "__main__":
    main()

