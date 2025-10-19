# app/db.py
import os
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

def get_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST','db'),
        port=int(os.getenv('DB_PORT',5432)),
        user=os.getenv('DB_USER','postgres'),
        password=os.getenv('DB_PASSWORD','1234'),
        dbname=os.getenv('DB_NAME','mydb')
    )

def upsert_bft41u(df):
    print("start saving data")
    ##Index(['Code', 'Name', 'TradeVolume', 'Transaction', 'TradeValue','TradePrice', 'BidVolume', 'AskVolume'],
    conn = get_conn()
    cur = conn.cursor()
    sql = """
    INSERT INTO bft41u_twse (trade_date, code, name, volume, trades, amount, price, last_bid, last_ask)
    VALUES %s
    ON CONFLICT (trade_date, code) DO UPDATE SET
      code = EXCLUDED.code,
      name = EXCLUDED.name,
      volume = EXCLUDED.volume,
      trades = EXCLUDED.trades,
      amount = EXCLUDED.amount,
      price = EXCLUDED.price,
      last_bid = EXCLUDED.last_bid,
      last_ask = EXCLUDED.last_ask;
    """
    records = []
    for _, r in df.iterrows():
        records.append((
            r['trade_date'],
            str(r['code']) if r['code'] is not None else None,
            r.get('stockname'),
            int(r['volume']) if pd.notna(r['volume']) else None,
            int(r['trades']) if pd.notna(r['trades']) else None,
            int(r['amount']) if pd.notna(r['amount']) else None,
            float(r['price']) if pd.notna(r['price']) else None,
            int(r['bid']) if pd.notna(r['bid']) else None,
            int(r['ask']) if pd.notna(r['ask']) else None,
        ))
    execute_values(cur, sql, records, template=None)
    conn.commit()
    cur.close()
    conn.close()
    
def upsert_twt84u(df):
    print("start saving data")
    conn = get_conn()
    cur = conn.cursor()
    sql = """
    INSERT INTO twt84u_twse (
        trade_date,
        code,
        name,
        today_limit_up,
        today_opening_price,
        today_limit_down,
        pre_opening_price,
        pre_price,
        pre_limit_up,
        pre_limit_down,
        last_trading_day,
        allow_odd_lot_trade
)
    VALUES %s
    ON CONFLICT (trade_date, code) DO UPDATE SET
    code = EXCLUDED.code,
    name = EXCLUDED.name,
    today_limit_up = EXCLUDED.today_limit_up,
    today_opening_price = EXCLUDED.today_opening_price,
    today_limit_down = EXCLUDED.today_limit_down,
    pre_opening_price = EXCLUDED.pre_opening_price,
    pre_price = EXCLUDED.pre_price,
    pre_limit_up = EXCLUDED.pre_limit_up,
    pre_limit_down = EXCLUDED.pre_limit_down,
    last_trading_day = EXCLUDED.last_trading_day,
    allow_odd_lot_trade = EXCLUDED.allow_odd_lot_trade;


    """
    records = []
    for _, r in df.iterrows():
        records.append((
            r['trade_date'],
            str(r['code']) if r['code'] is not None else None,
            r.get('name'),
            float(r['today_limit_up']) if pd.notna(r['today_limit_up']) else None,
            float(r['today_opening_price']) if pd.notna(r['today_opening_price']) else None,
            float(r['today_limit_down']) if pd.notna(r['today_limit_down']) else None,
            float(r['pre_opening_price']) if pd.notna(r['pre_opening_price']) else None,
            float(r['pre_price']) if pd.notna(r['pre_price']) else None,
            float(r['pre_limit_up']) if pd.notna(r['pre_limit_up']) else None,
            float(r['pre_limit_down']) if pd.notna(r['pre_limit_down']) else None,
            r.get('last_trading_day'),
            r.get('allow_odd_lot_trade')
            
        ))
    execute_values(cur, sql, records, template=None)
    conn.commit()
    cur.close()
    conn.close()

def initial():
    print("initailize database")
    conn = get_conn()
    cur = conn.cursor()
    sql = """
    DROP TABLE IF EXISTS bft41u_twse;
    DROP TABLE IF EXISTS twt84u_twse;
    DROP TABLE IF EXISTS fixp_tpex;

    CREATE TABLE IF NOT EXISTS bft41u_twse (
    trade_date DATE NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    volume BIGINT,
    trades BIGINT,
    amount BIGINT,
    price NUMERIC,
    last_bid BIGINT,
    last_ask BIGINT,
    PRIMARY KEY (trade_date, code)
    );

    CREATE TABLE IF NOT EXISTS twt84u_twse (
    trade_date DATE NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    today_limit_up NUMERIC,
    today_opening_price NUMERIC,
    today_limit_down NUMERIC,
    pre_opening_price NUMERIC,
    pre_price NUMERIC,
    pre_limit_up NUMERIC,
    pre_limit_down NUMERIC,
    last_trading_day DATE,
    allow_odd_lot_trade TEXT,

    PRIMARY KEY (trade_date, code)
    );

    CREATE TABLE IF NOT EXISTS fixp_tpex (
    trade_date DATE NOT NULL,
    code TEXT NOT NULL,
    name TEXT,
    volume BIGINT,
    trades BIGINT,
    amount BIGINT,
    price NUMERIC,
    PRIMARY KEY (trade_date, code)
    );


    """
   
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
