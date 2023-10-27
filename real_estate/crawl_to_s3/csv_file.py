from dotenv import load_dotenv
from datetime import datetime
import requests
import zipfile
import os
from crawl_utilities import upload_to_s3

MAIN_FOLDER = 'real_estate_price'


def get_price_information(url, year, season):
    """Get price information and upload it to AWS s3"""
    print("Dealing with:", year, season, "; url", url)
    res = requests.get(url)
    print("Status Code:", res.status_code, "; Content Length:", len(res.content))

    # create folders and files
    if res.status_code != 200 or len(res.content) < 5000:  # Do not have data yet
        print(f"No data available for {year} Season {season}. Stopping.")
        return
    else:
        fname = f"{year}{season}.zip"
        sub_folder = f"{MAIN_FOLDER}/unzipped_{year}_{season}"
        os.makedirs(sub_folder, exist_ok=True)

        zip_path = os.path.join(MAIN_FOLDER, fname)
        with open(zip_path, 'wb') as file:
            file.write(res.content)

        # upload zip to s3
        bucket_name = 'appworks.personal.project'
        upload_to_s3(zip_path, bucket_name, object_name=f"{MAIN_FOLDER}/{fname}")

        # unzip zip file and upload to s3
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                print("Extracting files...")
                zip_ref.extractall(sub_folder)
                for file_info in zip_ref.infolist():
                    extracted_file_path = os.path.join(sub_folder, file_info.filename)
                    print(f"Processing {extracted_file_path}...")

                    # same path with local
                    s3_object_name = f"{sub_folder}/{file_info.filename}"
                    print(f"Uploading {extracted_file_path} to S3 as {s3_object_name}...")
                    success = upload_to_s3(extracted_file_path, bucket_name, object_name=s3_object_name)
                    if not success:
                        print(f"Failed to upload {extracted_file_path} to S3.")
        except Exception as e:
            print(f"error: {str(e)}")


def find_missing_files_in_folder(folder):
    # Get current year and season
    now = datetime.now()
    current_year = now.year - 1911
    current_season = (now.month - 1) // 3 + 1

    # Generate expected file names
    expected_files = set()
    for year in range(current_year, 108, -1):
        for season in range(1, 5):
            if not (year == current_year and season > current_season):
                file_name = f"{MAIN_FOLDER}/unzipped_{year}_{season}"
                expected_files.add(file_name)

    # Check if folder exists and get existing file names
    existing_files = set()
    if os.path.exists(folder):
        file_names = os.listdir(folder)
        for name in file_names:
            if name.startswith('unzipped_'):
                transformed_name = f"{MAIN_FOLDER}/{name}"
                existing_files.add(transformed_name)

    # Calculate missing files
    missing_files = expected_files - existing_files
    print('Expected local files: ', expected_files)
    print('Existing local files: ', existing_files)
    print('Missing files: ', missing_files)
    return missing_files


def main():
    load_dotenv()

    # check missing file
    missing_files = find_missing_files_in_folder(MAIN_FOLDER)

    # Generate URLs for the missing files and download and decompress the data.
    for missing_file in missing_files:
        parts = missing_file.split('_')
        year = int(parts[-2])
        season = int(parts[-1])
        url = f"https://plvr.land.moi.gov.tw//DownloadSeason?season={year}S{season}&type=zip&fileName=lvr_landcsv.zip"
        get_price_information(url, year, season)


if __name__ == "__main__":
    main()


