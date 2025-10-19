# app/fetcher.py
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
import pytz

def clean_twse_df(df,date_yyyymmdd,type):
    if(type == "bft41u"):
        df=df.copy()
        df["trade_date"] = date_yyyymmdd
        cols = ["trade_date"] + [c for c in df.columns if c != "trade_date"]
        df = df[cols]
        df = df.rename(columns={
            "trade_date": "trade_date",
            "Code": "code",
            "Name": "stockname",
            "TradeVolume": "volume",
            "Transaction": "trades",
            "TradeValue":"amount",
            "TradePrice": "price",
            "BidVolume": "bid",
            "AskVolume": "ask"
        })

        int_columns = ["volume","amount","trades"]
        for col in int_columns:
                df[col]=df[col].str.replace(",","").replace("", "0").astype(int)
        
        float_columns = ["price","bid","ask"]
        for col in float_columns:
                df[col]=df[col].str.replace(",","").replace("", "0").astype(float)
        ##去除null
        
        print(df.head())
    if(type == "twt84u"):
        df=df.copy()
        df["trade_date"] = date_yyyymmdd
        cols = ["trade_date"] + [c for c in df.columns if c != "trade_date"]
        df = df[cols]
        df = df.rename(columns={
            "trade_date":"trade_date",
            "Code":"code",
            "Name":"name",
            "TodayLimitUp":"today_limit_up",
            "TodayOpeningRefPrice":"today_opening_price",
            "TodayLimitDown":"today_limit_down",
            "PreviousDayOpeningRefPrice":"pre_opening_price",
            "PreviousDayPrice":"pre_price",
            "PreviousDayLimitUp":"pre_limit_up",
            "PreviousDayLimitDown":"pre_limit_down",
            "LastTradingDay":"last_trading_day",
            "AllowOddLotTrade":"allow_odd_lot_trade"
        })
        
        float_columns = ["today_limit_up",
                         "today_opening_price",
                         "today_limit_down",
                         "pre_opening_price",
                         "pre_price",
                         "pre_limit_up",
                         "pre_limit_down"]
        date_columns = ["last_trading_day"]

        for col in float_columns:
                df[col]=(
                    df[col]
                    .str.replace(",","")
                    .replace({"": "0", "--": "0"})
                    .astype(float)
                )
        for col in date_columns:
                df[col]=parse_minguo_date( df[col])

        ##去除null


        print(df.head())


    return df

def parse_minguo_date(date_list):
    results = []
    for s in date_list:
        try:
            year, month, day = map(int, s.split('.'))
            results.append(datetime(year + 1911, month, day).date())
        except Exception as e:
            print(f"❌ 日期轉換失敗: {s} ({e})")
            results.append(None)  # 保持長度一致
    return results



##def clean_twse_df(df, date_yyyymmdd):
    # 嘗試把常見欄位對齊，依實際欄位微調
    colmap = {c: c.strip() for c in df.columns}
    df.rename(columns=colmap, inplace=True)
    # 範例常見欄位 (不同版本可能差異，需要按實際調整)
    # 例如：'證券代號','證券名稱','成交數量','成交筆數','成交金額','成交價','最後揭示買量','最後揭示賣量'
    name_map = {}
    for c in df.columns:
        if '證券代號' in c or 'Code' in c or '證券代號' == c:
            name_map[c] = 'code'
        if '證券名稱' in c or 'Name' in c:
            name_map[c] = 'name'
        if '成交數量' in c or 'Volume' in c:
            name_map[c] = 'volume'
        if '成交筆數' in c or 'Trades' in c:
            name_map[c] = 'trades'
        if '成交金額' in c or 'Amount' in c:
            name_map[c] = 'amount'
        if ('成交價' in c) or ('Price' in c) or ('成交均價' in c):
            name_map[c] = 'price'
        if '最後揭示買量' in c:
            name_map[c] = 'last_bid'
        if '最後揭示賣量' in c:
            name_map[c] = 'last_ask'
    df = df.rename(columns=name_map)
    # 清理數字（移除千分符號、空字串轉 None）
    for col in ['volume','trades','amount','last_bid','last_ask']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(',','').replace({'':'0','-':'0'}).apply(pd.to_numeric, errors='coerce').astype('Int64')
    if 'price' in df.columns:
        df['price'] = pd.to_numeric(df['price'].astype(str).str.replace(',',''), errors='coerce')
    # 加上 trade_date
    df['trade_date'] = pd.to_datetime(date_yyyymmdd, format='%Y%m%d').date()
    # 選取需要的欄位
    outcols = ['trade_date','code','name','volume','trades','amount','price','last_bid','last_ask']
    for c in outcols:
        if c not in df.columns:
            df[c] = None
    return df[outcols]

def fetch_tpex_fixp(date_yyyymmdd):
    """
    嘗試使用 tpex 的資料 API（o=data），日期格式可改為 yyyy-mm-dd 或 yyyymmdd 皆嘗試
    """
    base = "https://www.tpex.org.tw/web/stock/aftertrading/off_market/fixp_result.php"
    # 先嘗試 'yyyy-mm-dd'，再嘗試 'yyyymmdd'
    ds = [f"{date_yyyymmdd[:4]}-{date_yyyymmdd[4:6]}-{date_yyyymmdd[6:]}",
          date_yyyymmdd]
    for d in ds:
        params = {'l':'zh-tw','o':'data','d':d}
        try:
            r = requests.get(base, params=params, timeout=15)
            if r.status_code != 200:
                continue
            # 伺服器可能回 JSON 或 CSV
            try:
                data = r.json()
                # 假設 data['aaData'] 或類似結構，嘗試轉成 dataframe
                if isinstance(data, dict) and 'aaData' in data:
                    df = pd.DataFrame(data['aaData'])
                    # 需要依照回傳欄位重命名
                    # TODO: 根據實際 JSON key 做 mapping，下面是假設
                    df.columns = [c.strip() for c in df.columns]
                    # 加上 trade_date
                    df['trade_date'] = pd.to_datetime(date_yyyymmdd, format='%Y%m%d').date()
                    return df
                else:
                    # 如果 data 是簡單 list-of-lists
                    df = pd.DataFrame(data)
                    df['trade_date'] = pd.to_datetime(date_yyyymmdd, format='%Y%m%d').date()
                    return df
            except ValueError:
                # 非 JSON，當成 CSV
                encs = ['utf-8','utf-8-sig','big5','cp950']
                # 依實際欄位做清理
                # 若欄位有 '代號' '名稱' '成交數量' ...
                # 這裡只做通用處理
                df.columns = [c.strip() for c in df.columns]
                df['trade_date'] = pd.to_datetime(date_yyyymmdd, format='%Y%m%d').date()
                return df
        except Exception:
            continue
    raise RuntimeError("Failed to fetch TPEX FIXP for " + date_yyyymmdd)
