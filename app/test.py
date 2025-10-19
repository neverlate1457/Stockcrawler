import requests

##設定 API URL
url = "https://www.twse.com.tw/exchangeReport/BFT41U"
params = {
    "response": "json",
    "date": "20251003",         # 查詢日期（格式：YYYYMMDD）
    "selectType": "01"          # 查詢類型（例如：02 表示上市公司）
}

##設定 headers 模擬瀏覽器行為（避免被擋）
headers = {
    "User-Agent": "Mozilla/5.0"
}

##發送 GET 請求
response = requests.get(url, params=params, headers=headers,verify=False)

##檢查回應狀態
if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print("請求失敗，狀態碼：", response.status_code)