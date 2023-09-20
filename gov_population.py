# 人口


import requests
import json


def create_url(start_time, last_date_queried):
    api_base_url = 'https://nstatdb.dgbas.gov.tw/dgbasAll/webMain.aspx?sdmx/A130201010/1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.1.1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.M/'
    updated_url = f"{api_base_url}&startTime={start_time}&endTime={last_date_queried}"
    return updated_url


def main():
    # 發送GET請求
    response = requests.get(create_url('2000-M1', '2024-M4'))  # 設定start, end time
    if response.status_code == 200:
        try:
            # 解析JSON數據
            data = json.loads(response.text)
            print('API Response:')
            print(json.dumps(data, indent=4, ensure_ascii=False))  # 美化輸出
            # print(json.dumps(data["data"]["dataSets"][0]["series"]["0"]["observations"], indent=4, ensure_ascii=False))
        except json.JSONDecodeError:
            print("Received data isn't a valid JSON. Printing raw data:")
            print(response.text)
    else:
        print(f"Failed to get data: {response.status_code}")
        return None


if __name__ == "__main__":
    main()