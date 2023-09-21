# 國民所得統計常用資料

import requests
import json
import os


def create_url(start_time, last_date_queried):
    api_base_url = 'https://nstatdb.dgbas.gov.tw/dgbasAll/webMain.aspx?sdmx/A018101010/1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.1.1+2+3+4+5+6+7+8+9+10+11+12+13+14+15.Ｑ/'
    updated_url = f"{api_base_url}&startTime={start_time}&endTime={last_date_queried}"
    return updated_url


def save_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def main():
    # 發送GET請求
    response = requests.get(create_url('2000-Q1', '2024-Q4'))  # 設定start, end time
    if response.status_code == 200:
        try:
            # 解析JSON數據
            data = json.loads(response.text)
            print('API Response:')
            print(json.dumps(data, indent=4, ensure_ascii=False))  # 美化輸出

            # 保存到JSON檔案
            save_path = os.path.join(os.getcwd(), 'gdp_folder')  # 設定保存路徑
            if not os.path.exists(save_path):
                os.makedirs(save_path)
            save_json(data, os.path.join(save_path, 'data.json'))

        except json.JSONDecodeError:
            print("Received data isn't a valid JSON. Printing raw data:")
            print(response.text)
    else:
        print(f"Failed to get data: {response.status_code}")
        return None


if __name__ == "__main__":
    main()
