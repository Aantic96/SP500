import os
import pytz
import yfinance as yf
import pandas as pd
import psycopg2
import argparse
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from math import ceil

load_dotenv(".env")

DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")

BATCH_SIZE = 20

def connect_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        exit(1)

    conn.commit()
    cur.close()
    conn.close()
    print("Tables 'stock_name' and 'price_data' ensured.")

def reset_tables():
        print("Resetting tables...")
        conn = connect_db()
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE price_data, stock_name RESTART IDENTITY CASCADE;")
        conn.commit()
        cur.close()
        conn.close()
        print("Tables reset.")

def get_sp500_symbols():
    print("Fetching S&P 500 company list...")
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    table = pd.read_html(url)[0]
    return table["Symbol"].tolist()

def insert_stock_names(symbols):
    conn = connect_db()
    cur = conn.cursor()

    stock_ids = {}
    for symbol in symbols:
        cur.execute("""
            INSERT INTO stock_name (name) VALUES (%s)
            ON CONFLICT (name) DO NOTHING
            RETURNING id;
        """, (symbol,))
        
        result = cur.fetchone()
        if result:
            stock_ids[symbol] = result[0]
        else:
            cur.execute("SELECT id FROM stock_name WHERE name = %s;", (symbol,))
            stock_ids[symbol] = cur.fetchone()[0]

    conn.commit()
    cur.close()
    conn.close()
    return stock_ids

def fetch_and_store(batch_size = BATCH_SIZE):
    symbols = get_sp500_symbols()
    print(f"Total symbols: {len(symbols)}")

    conn = connect_db()
    cur = conn.cursor()

    for symbol in symbols:
        cur.execute("""
            INSERT INTO stock_name (name) VALUES (%s)
            ON CONFLICT (name) DO NOTHING;
        """, (symbol,))
    conn.commit()

    cur.execute("SELECT id, name FROM stock_name;")
    id_map = {name: sid for sid, name in cur.fetchall()}

    total_batches = ceil(len(symbols) / batch_size)

    for i in range(total_batches):
        batch = symbols[i * batch_size : (i + 1) * batch_size]
        print(f"\n Batch {i+1}/{total_batches}: {batch}")

        try:
            data = yf.download(batch, period="1d", group_by='ticker', threads=True)
        except Exception as e:
            print(f"Failed to fetch batch {i+1}: {e}")
            continue

        for symbol in batch:
            try:
                if symbol not in data or data[symbol].empty:
                    print(f"No data for {symbol}")
                    continue

                df = data[symbol]
                latest = df.iloc[-1]
                stock_id = id_map.get(symbol)

                if not stock_id:
                    print(f"No stock_id for {symbol}")
                    continue

                timestamp = latest.name
                if timestamp.tzinfo is not None:
                    timestamp = timestamp.tz_convert('UTC').replace(tzinfo=None)
                else:
                    timestamp = timestamp.replace(tzinfo=None)

                row = (
                    stock_id,
                    timestamp,
                    float(latest["Open"]),
                    float(latest["High"]),
                    float(latest["Low"]),
                    float(latest["Close"]),
                    int(latest["Volume"]) if not pd.isna(latest["Volume"]) else None
                )

                cur.execute("""
                    INSERT INTO price_data (stock_id, date, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (stock_id, date) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume;
                """, row)

                print(f"{symbol} added")

            except Exception as e:
                print(f"Error with {symbol}: {e}")

        conn.commit()

    cur.close()
    conn.close()
    print("Batching finished")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="S&P 500 Stock Data Ingestion")
    parser.add_argument("--truncate", action="store_true", help="Reset database tables before fetching")
    args = parser.parse_args()

    if args.truncate:
        reset_tables()

    print("Starting stock data ingestion...")

    fetch_and_store()
    print("Completed!")