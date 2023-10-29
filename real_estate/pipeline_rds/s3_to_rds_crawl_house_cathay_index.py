from dotenv import load_dotenv
import boto3
import json
import os
from utilities.utils import connect_to_db


def download_file_from_s3(bucket_name, object_key, file_name):
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket_name, object_key, file_name)
        print(f"File downloaded from S3: {file_name}")
        return True
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        return False


def main():
    load_dotenv()
    bucket_name = 'appworks.personal.project'
    object_key = 'crawl_to_s3_file/house_cathay_index.json'  # the name of S3 file
    file_name = "download_from_s3_file/house_cathay_index.json"  # local

    directory = "download_from_s3_file"
    if not os.path.exists(directory):
        os.makedirs(directory)

    # S3 download
    download_file_from_s3(bucket_name, object_key, file_name)

    with open(file_name, 'r', encoding='utf-8') as f:
        house_cathay_index = json.load(f)
    print(json.dumps(house_cathay_index, indent=4, ensure_ascii=False))

    # create house index db
    conn = connect_to_db("house_cathy_index")
    try:
        with conn.cursor() as cursor:
            # Create a new table for Cathay house index.
            cursor.execute("""
                        CREATE TABLE IF NOT EXISTS house_cathay_index (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            period VARCHAR(255),
                            city VARCHAR(255),
                            index_value FLOAT,
                            UNIQUE (period, city)
                        )
                    """)
            conn.commit()

            data_to_insert = []
            for data in house_cathay_index:
                period = data["年度季別"]
                for city, index_value in data.items():
                    if city != "年度季別":
                        data_to_insert.append((period, city, index_value))
            print(data_to_insert)

            # Insert data into the new table.(ON DUPLICATE KEY UPDATE)
            cursor.executemany("""
                        INSERT INTO house_cathay_index (period, city, index_value)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        index_value = VALUES(index_value)
                    """, data_to_insert)
            conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()