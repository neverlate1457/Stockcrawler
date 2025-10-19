# app/main.py
import requests
import os
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from fetcher import clean_twse_df
from db import upsert_bft41u, upsert_twt84u , initial
import pandas as pd
import pytz
import argparse


TZ = os.getenv('TZ', 'Asia/Taipei')
sched = BlockingScheduler(timezone=TZ)
##每日排程

def job_for_today():
    today = datetime.now(pytz.timezone(TZ)).strftime("%Y%m%d")
    print("Running job for", today)

    
    try:
        df = fetch_twse_bft41u(today)
        print(df.head())
        try:
            ##資料處理
            df_twse = clean_twse_df(df,today)
            print("寫入資料庫")
            upsert_bft41u(df_twse)
            print("TWSE done:", len(df_twse))
        except Exception as e:
            print("DB error:", e)
    except Exception as e:
        print("TWSE error:", e)
    ##df = fetch_latest_available(today)
    
    # TWSE
    
    # (同理可以呼叫 TPEX 的 fetch 並 upsert 到其 table)
    #df_tpex = fetch_tpex_fixp(today)
    # upsert_tpex(df_tpex)  # 若實作 upsert_tpex

def fetch_twse_bft41u(date: str) -> pd.DataFrame:
    """
    嘗試抓取台灣證交所 BFT41U 報表
    :param date: 日期 (格式 YYYYMMDD，例如 '20251003')
    :return: Pandas DataFrame
    api1:https://openapi.twse.com.tw/v1/exchangeReport/BFT41U 這個沒有參數
    """
    url = f"https://www.twse.com.tw/exchangeReport/BFT41U"
    
    headers = {
    "User-Agent": "Mozilla/5.0"
}

    params = {
    "date": date,
    "response": "json",
    "selectType": "ALLBUT0999"   # ALL 全部不含權證
}

    


    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.SSLError:
        print("⚠️ SSL 驗證失敗，改用 verify=False（僅測試用）")
        resp = requests.get(url, timeout=10,params=params, headers=headers, verify=False)
        resp.raise_for_status()
        
    data = resp.json()
    ##print(data)
    if not data:
        raise Exception(f"No data returned for {date}")
    if not data or "data" not in data or "fields" not in data:
        raise Exception(f"No valid data returned for {date}")
    df = pd.DataFrame(data["data"], columns=
        [
        "Code",
        "Name",
        "TradeVolume",
        "Transaction",
        "TradeValue",
        "TradePrice",
        "BidVolume",
        "AskVolume"
        ])
    return df

def fetch_twse_twt84u(date: str) -> pd.DataFrame:
    """
    嘗試抓取台灣證交所 twt84u 報表
    :param date: 日期 (格式 YYYYMMDD，例如 '20251003')
    :return: Pandas DataFrame
    api1:https://openapi.twse.com.tw/v1/exchangeReport/TWT84U 這個沒有參數
    """
    url = f"https://www.twse.com.tw/exchangeReport/TWT84U"
    
    headers = {
    "User-Agent": "Mozilla/5.0"
}

    params = {
    "date": date,
    "response": "json",
    "selectType": "ALLBUT0999"   # ALL 全部不含權證
}

    


    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.SSLError:
        print("⚠️ SSL 驗證失敗，改用 verify=False（僅測試用）")
        resp = requests.get(url, timeout=10,params=params, headers=headers, verify=False)
        resp.raise_for_status()
        
    data = resp.json()
    ##print(data)
    if not data:
        raise Exception(f"No data returned for {date}")
    if not data or "data" not in data or "fields" not in data:
        raise Exception(f"No valid data returned for {date}")
    df = pd.DataFrame(data["data"], columns=
        [
        "Code",
        "Name",
        "TodayLimitUp",
        "TodayOpeningRefPrice",
        "TodayLimitDown",
        "PreviousDayOpeningRefPrice",
        "PreviousDayPrice",
        "PreviousDayLimitUp",
        "PreviousDayLimitDown",
        "LastTradingDay",
        "AllowOddLotTrade"
        ])
    return df

def fetch_latest_available(date: str) -> pd.DataFrame:
    """
    從指定日期往前回溯，找到最近一個有資料的交易日
    """
    tz = pytz.timezone(TZ)
    dt = datetime.strptime(date, "%Y%m%d").date()

    for i in range(10):  # 最多往前找 10 天
        day_str = dt.strftime("%Y%m%d")
        try:
            df = fetch_twse_bft41u(day_str)
            print(f"✅ 成功抓到 {day_str} 的資料")
            print(df.columns)

            return df
        except Exception as e:
            print(f"❌ {day_str} 沒有資料，往前一天… ({e})")
            dt -= timedelta(days=1)

    raise Exception("連續 10 天都沒有資料，請檢查 API 或日期設定")

def get_history_data(date_str: str,interval : int) -> pd.DataFrame:
    
    
     
    date = datetime.strptime(date_str, "%Y-%m-%d") - timedelta(interval)
    for i in range(interval):
        try:
            df = fetch_twse_bft41u(date.strftime("%Y%m%d"))
            ##print(df.columns)
         ##資料處理
        except Exception as e:
            print(f"❌ {date.strftime('%Y%m%d')} 沒有資料({e})")
        else:
            df_twse_bft41u = clean_twse_df(df,date,"bft41u")
            print("寫入資料庫")
            upsert_bft41u(df_twse_bft41u)
            print("History data loaded:", date)
            print("TWSE done:", len(df_twse_bft41u))
        
        try:
            df = fetch_twse_twt84u(date.strftime("%Y%m%d"))
            ##print(df.columns)
         ##資料處理
        except Exception as e:
            print(f"❌ {date.strftime('%Y%m%d')} 沒有資料({e})")
        else:
            df_twse_twt84u = clean_twse_df(df,date,"twt84u")
            print("寫入資料庫")
            upsert_twt84u(df_twse_twt84u)
            print("History data loaded:", date)
            print("TWSE done:", len(df_twse_twt84u))
        
        date=date+timedelta(days=1)
    
@sched.scheduled_job('cron', hour=14, minute=14)  # 每日 18:05 Asia/Taipei（可用 env 調整）

def scheduled_job():
    print("現在時間 18:05")
    job_for_today()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="system on")
    subparsers = parser.add_subparsers(dest="command")

    history_parser = subparsers.add_parser("que", help="查詢歷史資料")
    history_parser.add_argument("--end", type=str, default=datetime.today().strftime("%Y-%m-%d"), help="結束日期 (預設今天)")
    history_parser.add_argument("--days", type=int, required=True, help="往前查幾天")

    toggle_parser = subparsers.add_parser("toggle", help="開啟或關閉每日爬蟲")
    toggle_parser.add_argument("--on", action="store_true", help="開啟爬蟲")
    toggle_parser.add_argument("--off", action="store_true", help="關閉爬蟲")

    time_parser = subparsers.add_parser("set-time", help="設定每日爬蟲時間")
    time_parser.add_argument("--hour", type=int, required=True, help="小時 (0-23)")
    time_parser.add_argument("--minute", type=int, required=True, help="分鐘 (0-59)")

    initial_parser = subparsers.add_parser("init", help="資料庫初始化")

    args = parser.parse_args()

    if args.command == "que":
        print("查詢歷史資料")
        get_history_data(args.end, args.days)
    elif args.command == "toggle":
        if args.on:
           print("開啟每日爬蟲")
           sched.start()
        elif args.off:
            print("關閉每日爬蟲")
            sched.shutdown()
        else:
            print("請加上 --on 或 --off")
    elif args.command == "set-time":
        print("設定時間")
        ##set_spider_time(args.hour, args.minute)
    elif args.command == "init":
        print("資料庫初始化")
        ##set_spider_time(args.hour, args.minute)
        initial()
    else:
        parser.print_help()
