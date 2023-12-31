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
    object_key = 'crawl_to_s3_file/construction_cost_data.json'  # S3 file name
    file_name = 'download_from_s3_file/construction_cost_data.json'  # local name

    directory = "download_from_s3_file"
    if not os.path.exists(directory):
        os.makedirs(directory)

    # S3 download
    download_file_from_s3(bucket_name, object_key, file_name)

    with open(file_name, 'r', encoding='utf-8') as f:
        construction_cost = json.load(f)
    print(json.dumps(construction_cost, indent=4, ensure_ascii=False))

    # create business cycle indicator db
    conn = connect_to_db("business cycle indicator")

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                        CREATE TABLE IF NOT EXISTS economic_construction_cost (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            time_id VARCHAR(255) UNIQUE,
                            time_name VARCHAR(255),
                            construction_index FLOAT COMMENT '營造工程總指數'
                        )
                    """)

            conn.commit()

            observations_0 = construction_cost['data']['dataSets'][0]['series']['0']['observations']
            time_structure = construction_cost['data']['structure']['dimensions']['observation'][0]['values']

            data_to_insert = []
            for idx, time_info in enumerate(time_structure):
                time_id = time_info['id']
                time_name = time_info['name']
                construction_index = observations_0[str(idx)][0]

                data_to_insert.append((time_id, time_name, construction_index))

            cursor.executemany("""
                        INSERT INTO economic_construction_cost (time_id, time_name, construction_index)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        time_name = VALUES(time_name),
                        construction_index = VALUES(construction_index)
                    """, data_to_insert)
            conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
