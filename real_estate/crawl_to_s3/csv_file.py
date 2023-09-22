import requests
import os
import zipfile
import time
import boto3


def upload_to_s3(file_name, bucket, object_name=None):
    """Uploads a file to an S3 bucket"""
    if object_name is None:
        object_name = file_name

    s3 = boto3.client('s3')

    try:
        s3.upload_file(file_name, bucket, object_name)
    except Exception as e:
        print("S3 Upload Error:", e)
        return False
    return True

def real_estate_crawler(year, season, s3_bucket):
    """Crawls real estate data for a given year and season and uploads to S3."""
    if year > 1000:
        year -= 1911

    fname = str(year) + str(season) + '.zip'
    main_folder = 'real_estate_price'
    sub_folder = f"{main_folder}/unzipped_{year}_{season}"

    if not os.path.isdir(main_folder):
        os.mkdir(main_folder)

    if not os.path.isdir(sub_folder):
        os.mkdir(sub_folder)

    res = requests.get("https://plvr.land.moi.gov.tw//DownloadSeason?season=" + str(year) + "S" + str(season) + "&type=zip&fileName=lvr_landcsv.zip")

    if res.status_code != 200 or len(res.content) < 100:
        print(f"No data available for {year} Season {season}. Stopping.")
        return

    zip_path = os.path.join(main_folder, fname)
    open(zip_path, 'wb').write(res.content)

    try:
        # Unzip the file into the subfolder
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(sub_folder)

        # Upload zip file to S3
        upload_to_s3(zip_path, s3_bucket, zip_path)

        # Upload unzipped files to S3
        for root, dirs, files in os.walk(sub_folder):
            for file in files:
                file_path = os.path.join(root, file)
                s3_object_name = os.path.join(sub_folder, file)  # Adjust the path as needed
                upload_to_s3(file_path, s3_bucket, s3_object_name)

    except zipfile.BadZipFile:
        print("Failed to unzip the file.")

    # Delay to avoid overloading the server
    time.sleep(10)


s3_bucket = 'appworks.personal.project'

# Loop through years and seasons
for year in range(105, 113):
    for season in range(1, 4):
        print(year, season)
        real_estate_crawler(year, season, s3_bucket)