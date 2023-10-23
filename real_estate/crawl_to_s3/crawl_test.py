from unittest.mock import patch, Mock
import requests
import pytest
import json
import os
from crawl_utilities import save_data_to_json_file, upload_to_s3, create_government_url
from csv_file import get_price_information


# unittest
def test_save_data_to_json_file(tmpdir):
    data = {"key": "value"}
    directory = tmpdir.strpath
    file_name = "test.json"
    save_data_to_json_file(data, directory, file_name)
    with open(os.path.join(directory, file_name), 'r') as f:
        loaded_data = json.load(f)
    assert loaded_data == data


def test_create_government_url():
    url = "http://example.com/api?param=value"
    start_time = "2022-01-01"
    end_time = "2022-01-31"
    expected = "http://example.com/api?param=value&startTime=2022-01-01&endTime=2022-01-31"
    result = create_government_url(url, start_time, end_time)
    assert result == expected


def test_csv_non_200_response(mocker):
    # Mock requests.get() to return a response with status_code 404
    mock_response = Mock(status_code=404, content=b'')
    mocker.patch('csv_file.requests.get', return_value=mock_response)

    # Mock the upload_to_s3 function
    mock_upload_to_s3 = mocker.patch('csv_file.upload_to_s3', return_value=True)

    # Call the function
    get_price_information("some_url", 2023, 1)

    # Assert that upload_to_s3 was not called
    mock_upload_to_s3.assert_not_called()


def test_csv_small_content_response(mocker):
    # Mock requests.get() to return a response with status_code 200 and small content
    mock_response = Mock(status_code=200, content=b'small content')
    mocker.patch('csv_file.requests.get', return_value=mock_response)

    # Mock the upload_to_s3 function
    mock_upload_to_s3 = mocker.patch('csv_file.upload_to_s3', return_value=True)

    # Call the function
    get_price_information("some_url", 2023, 1)

    # Assert that upload_to_s3 was not called
    mock_upload_to_s3.assert_not_called()


def test_csv_valid_response(mocker):
    # Mock requests.get() to return a valid response with status_code 200 and larger content
    mock_response = Mock(status_code=200, content=b'large content' * 1000)
    mocker.patch('csv_file.requests.get', return_value=mock_response)

    # Mock the upload_to_s3 function
    mock_upload_to_s3 = mocker.patch('csv_file.upload_to_s3', return_value=True)

    # Call the function
    get_price_information("some_url", 2023, 1)

    # Assert that upload_to_s3 was called
    mock_upload_to_s3.assert_called()


@patch('boto3.client')
def test_upload_to_s3_mocked(mock_boto3_client):
    # Create a mock for the s3 client
    mock_s3 = Mock()
    mock_boto3_client.return_value = mock_s3

    # Call function
    upload_to_s3("test_file", "test_bucket")

    # Check if s3.upload_file was called with expected arguments
    mock_s3.upload_file.assert_called_once_with("test_file", "test_bucket", "test_file")


# Integration test
urls = [
    ('https://news.cnyes.com/news/cat/tw_housenews', 'text/html'),  # anue
    ('https://pip.moi.gov.tw/V3/E/SCRE0201.aspx', 'text/html'),  # house_cathy
    ('https://pip.moi.gov.tw/V3/E/SCRE0201.aspx', 'text/html'),  # mortgage_interest
    ('https://nstatdb.dgbas.gov.tw/dgbasAll/webMain.aspx?sdmx/A120101010/1+2+3+4+5.1.1.M/', 'application/json'),  # business cycle
    ('https://nstatdb.dgbas.gov.tw/dgbasAll/webMain.aspx?sdmx/A030501015/1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.1.1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.M/', 'application/json'),  # construction_cost
    ('https://nstatdb.dgbas.gov.tw/dgbasAll/webMain.aspx?sdmx/A018101010/1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.1.1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.Ｑ/', 'application/json'),  # gdp
    ('https://nstatdb.dgbas.gov.tw/dgbasAll/webMain.aspx?sdmx/A130201010/1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.1.1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.M/', 'application/json')  # population
]


@pytest.mark.parametrize('url, expected_content_type', urls)
def test_urls_response(url, expected_content_type):
    response = requests.get(url)
    assert response.status_code == 200
    assert expected_content_type in response.headers['Content-Type']



