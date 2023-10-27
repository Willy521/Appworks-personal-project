import boto3
import os
import json


def download_file_from_s3(bucket_name, object_key, file_name):
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket_name, object_key, file_name)
        print(f"File downloaded from S3: {file_name}")
        return True
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        return False


def download_and_print_json_from_s3(directory, bucket_name, object_key, file_name):
    """Download a JSON file from S3, and print its content in a formatted manner."""
    if not os.path.exists(directory):
        os.makedirs(directory)
    download_file_from_s3(bucket_name, object_key, file_name)
    with open(file_name, 'r', encoding='utf-8') as f:
        data = json.load(f)
        formatted_json = json.dumps(data, ensure_ascii=False, indent=4)
        print(formatted_json)
        return data
