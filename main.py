import os
import requests
import datetime
import base64
import time
import math
from typing import List, Dict, Optional
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.backends import default_backend
from supabase import create_client, Client


# ==========================================================
# CONFIG (Render Environment Variables)
# ==========================================================

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
BASE_URL = "https://api.elections.kalshi.com"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

TABLE_NAME = "kalshi_high_snapshots"

SERIES_TICKERS = [
    "KXHIGHNY", "KXHIGHMIA", "KXHIGHAUS", "KXHIGHCHI",
    "KXHIGHLAX", "KXHIGHTDC", "KXHIGHTDAL", "KXHIGHTATL",
    "KXHIGHPHIL", "KXHIGHDEN", "KXHIGHTSEA", "KXHIGHTSFO",
    "KXHIGHTLV", "KXHIGHTHOU", "KXHIGHTPHX", "KXHIGHTNOLA",
    "KXHIGHTBOS", "KXHIGHTMIN", "KXHIGHTOKC", "KXHIGHTSATX"
]


# ==========================================================
# AUTH
# ==========================================================

def load_private_key_from_env():
    private_key_str = os.getenv("KALSHI_PRIVATE_KEY")
    if not private_key_str:
        raise ValueError("Missing KALSHI_PRIVATE_KEY environment variable")

    return serialization.load_pem_private_key(
        private_key_str.encode(),
        password=None,
        backend=default_backend()
    )

def sign_message(private_key: rsa.RSAPrivateKey, message: str) -> str:
    signature = private_key.sign(
        message.encode(),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256()
    )
    return base64.b64encode(signature).decode()

def get_headers(method: str, path: str, private_key):
    timestamp = str(int(datetime.datetime.now(datetime.UTC).timestamp() * 1000))
    path_without_query = path.split("?")[0]
    message = timestamp + method + path_without_query
    signature = sign_message(private_key, message)

    return {
        "KALSHI-ACCESS-KEY": API_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp
    }


# ==========================================================
# API FETCHERS
# ==========================================================

def get_open_events(series, private_key):
    path = f"/trade-api/v2/events?series_ticker={series}&status=open&limit=100"
    headers = get_headers("GET", path, private_key)
    r = requests.get(BASE_URL + path, headers=headers)
    return r.json().get("events", [])

def get_open_markets(event_ticker, private_key):
    path = f"/trade-api/v2/markets?event_ticker={event_ticker}&status=open&limit=100"
    headers = get_headers("GET", path, private_key)
    r = requests.get(BASE_URL + path, headers=headers)
    return r.json().get("markets", [])

def get_orderbook(market_ticker, private_key):
    path = f"/trade-api/v2/markets/{market_ticker}/orderbook"
    headers = get_headers("GET", path, private_key)
    r = requests.get(BASE_URL + path, headers=headers)
    return r.json().get("orderbook", {}) or {}


# ==========================================================
# DEPTH PROCESSING
# ==========================================================

def extract_depth(orderbook):

    yes_raw = orderbook.get("yes") or []
    no_raw = orderbook.get("no") or []

    yes_bids = list(reversed(yes_raw))[:5]
    no_bids = list(reversed(no_raw))[:5]

    yes_asks = sorted([[100 - p, q] for p, q in no_bids], key=lambda x: x[0])[:5]

    return yes_bids, yes_asks


# ==========================================================
# MAIN SNAPSHOT LOGIC
# ==========================================================

def run_snapshot():

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    private_key = load_private_key_from_env()

    timestamp = datetime.datetime.now(datetime.UTC).isoformat()

    rows_to_insert = []

    for series in SERIES_TICKERS:

        events = get_open_events(series, private_key)

        for event in events:

            event_ticker = event.get("event_ticker") or event.get("ticker")
            markets = get_open_markets(event_ticker, private_key)

            event_markets = []

            for market in markets:

                market_ticker = market.get("ticker")
                if not market_ticker:
                    continue

                suffix = market_ticker.split("-")[-1]

                if not (suffix.startswith("B") or suffix.startswith("T")):
                    continue

                event_markets.append({
                    "ticker": market_ticker,
                    "suffix": suffix
                })

            t_markets = sorted(
                [m for m in event_markets if m["suffix"].startswith("T")],
                key=lambda x: int(float(x["suffix"][1:]))
            )

            b_markets = sorted(
                [m for m in event_markets if m["suffix"].startswith("B")],
                key=lambda x: int(float(x["suffix"][1:]))
            )

            ordered_markets = []

            if len(t_markets) > 0:
                ordered_markets.append(t_markets[0])

            ordered_markets.extend(b_markets)

            if len(t_markets) > 1:
                ordered_markets.append(t_markets[-1])

            for idx, market_data in enumerate(ordered_markets):

                market_ticker = market_data["ticker"]
                suffix = market_data["suffix"]
                order_number = idx + 1

                bucket_type = None
                lower_bound = None
                upper_bound = None

                value = int(float(suffix[1:]))

                if suffix.startswith("B"):
                    bucket_type = "range"
                    lower_bound = value
                    upper_bound = value + 1

                elif suffix.startswith("T"):
                    if market_data == t_markets[0]:
                        bucket_type = "below"
                        upper_bound = value - 1
                    else:
                        bucket_type = "above"
                        lower_bound = value + 1

                orderbook = get_orderbook(market_ticker, private_key)
                yes_bids, yes_asks = extract_depth(orderbook)

                row = {
                    "timestamp_utc": timestamp,
                    "event": event_ticker,
                    "market": suffix,
                    "bucket_type": bucket_type,
                    "lower_bound": lower_bound,
                    "upper_bound": upper_bound,
                    "order": order_number
                }

                for i in range(5):
                    if i < len(yes_bids):
                        row[f"bid{i+1}_price"] = yes_bids[i][0]
                        row[f"bid{i+1}_qty"] = yes_bids[i][1]
                    else:
                        row[f"bid{i+1}_price"] = None
                        row[f"bid{i+1}_qty"] = None

                for i in range(5):
                    if i < len(yes_asks):
                        row[f"ask{i+1}_price"] = yes_asks[i][0]
                        row[f"ask{i+1}_qty"] = yes_asks[i][1]
                    else:
                        row[f"ask{i+1}_price"] = None
                        row[f"ask{i+1}_qty"] = None

                rows_to_insert.append(row)

    if rows_to_insert:
        supabase.table(TABLE_NAME).insert(rows_to_insert).execute()
        print(f"[{timestamp}] Inserted {len(rows_to_insert)} rows.")
    else:
        print(f"[{timestamp}] No rows to insert.")


# ==========================================================
# 5-MINUTE ALIGNMENT LOOP
# ==========================================================

def sleep_until_next_5_min_mark():

    now = datetime.datetime.now(datetime.UTC)
    minute = now.minute

    next_minute = (math.floor(minute / 5) + 1) * 5

    if next_minute == 60:
        next_run = now.replace(hour=(now.hour + 1) % 24, minute=0, second=0, microsecond=0)
    else:
        next_run = now.replace(minute=next_minute, second=0, microsecond=0)

    sleep_seconds = (next_run - now).total_seconds()

    print(f"Sleeping {int(sleep_seconds)} seconds until {next_run}")
    time.sleep(max(0, sleep_seconds))


# ==========================================================
# ENTRY POINT
# ==========================================================

if __name__ == "__main__":

    print("Starting 5-minute aligned Kalshi weather ladder worker...")

    while True:
        sleep_until_next_5_min_mark()

        try:
            run_snapshot()
        except Exception as e:
            print("Error occurred:", e)
