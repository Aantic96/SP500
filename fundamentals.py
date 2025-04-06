import time
import yfinance as yf
from psycopg2.extras import execute_values
from utils import connect_db, get_sp500_symbols
from math import ceil

def main(sleep_seconds = 0.25, batch_size = 20):
    print("Starting fundamentals ingestion...")

    symbols = get_sp500_symbols()
    total_batches = ceil(len(symbols) / batch_size)
    conn = connect_db()
    cur = conn.cursor()

    cur.execute("SELECT id, name FROM stock_name;")
    id_map = {name: sid for sid, name in cur.fetchall()}

    cur.close()
    conn.close()

    for i in range(total_batches):
        batch = symbols[i * batch_size : (i + 1) * batch_size]
        print(f"\nProcessing batch {i+1}/{total_batches}: {batch}")

        rows = []

        for symbol in batch:
            stock_id = id_map.get(symbol)
            if not stock_id:
                print(f"No stock ID for {symbol}, skipping.")
                continue

            try:
                ticker = yf.Ticker(symbol)
                info = ticker.info

                pe_ratio = info.get("trailingPE")
                pb_ratio = info.get("priceToBook")
                market_cap = info.get("marketCap")

                rows.append((stock_id, pe_ratio, pb_ratio, market_cap))
                print(f"{symbol}: fundamentals prepared")

            except Exception as e:
                print(f"Error fetching {symbol}: {e}")

            time.sleep(sleep_seconds)

        if rows:
            conn = connect_db()
            cur = conn.cursor()

            query = """
                INSERT INTO fundamentals (stock_id, pe_ratio, pb_ratio, market_cap)
                VALUES %s
                ON CONFLICT (stock_id) DO UPDATE SET
                    pe_ratio = EXCLUDED.pe_ratio,
                    pb_ratio = EXCLUDED.pb_ratio,
                    market_cap = EXCLUDED.market_cap;
            """

            try:
                execute_values(cur, query, rows)
                conn.commit()
                print(f"Inserted {len(rows)} fundamentals.")
            except Exception as e:
                print(f"Insert failed for batch {i+1}: {e}")
            finally:
                cur.close()
                conn.close()

    print("Fundamentals fetching complete.")

if __name__ == "__main__":
    main()