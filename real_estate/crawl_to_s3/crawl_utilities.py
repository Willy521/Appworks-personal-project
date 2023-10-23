import json
import os
import boto3


# upload to s3
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


# create government url
def create_government_url(api_base_url, start_time, last_date_queried):
    updated_url = f"{api_base_url}&startTime={start_time}&endTime={last_date_queried}"
    return updated_url


# def extract_data_from_response(response):
def save_data_to_json_file(data, directory, file_name):
    """Save the given data to a JSON file."""
    if not os.path.exists(directory):
        os.makedirs(directory)
    json_file_path = os.path.join(directory, file_name)
    with open(json_file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)