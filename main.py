import requests
import datetime
import base64
from typing import List, Dict
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.backends import default_backend
from supabase import create_client, Client


# ==========================================================
# CONFIG
# ==========================================================

import os

API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
BASE_URL = "https://api.elections.kalshi.com"
SERIES_TICKER = "KXHIGHNY"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

TABLE_NAME = "nyc_high_snapshots"


# ==========================================================
# AUTH HELPERS
# ==========================================================

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


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
        message.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH
        ),
        hashes.SHA256()
    )
    return base64.b64encode(signature).decode("utf-8")


def get_auth_headers(method: str, path: str, private_key) -> Dict[str, str]:
    timestamp = str(int(datetime.datetime.now().timestamp() * 1000))
    path_without_query = path.split("?")[0]
    message = timestamp + method + path_without_query
    signature = sign_message(private_key, message)

    return {
        "KALSHI-ACCESS-KEY": API_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp
    }


# ==========================================================
# FETCH FUNCTIONS
# ==========================================================

def get_all_open_events(private_key) -> List[Dict]:
    path = f"/trade-api/v2/events?series_ticker={SERIES_TICKER}&status=open&limit=100"
    headers = get_auth_headers("GET", path, private_key)
    response = requests.get(BASE_URL + path, headers=headers)
    return response.json().get("events", [])


def get_markets_for_event(event_ticker: str, private_key) -> List[Dict]:
    path = f"/trade-api/v2/markets?event_ticker={event_ticker}&limit=100"
    headers = get_auth_headers("GET", path, private_key)
    response = requests.get(BASE_URL + path, headers=headers)
    return response.json().get("markets", [])


def get_market_depth(market_ticker: str, private_key) -> Dict:
    path = f"/trade-api/v2/markets/{market_ticker}/orderbook"
    headers = get_auth_headers("GET", path, private_key)
    response = requests.get(BASE_URL + path, headers=headers)

    orderbook = response.json().get("orderbook", {})

    yes_bids = orderbook.get("yes") or []
    no_bids = orderbook.get("no") or []

    yes_bids = list(reversed(yes_bids))[:5]
    no_bids = list(reversed(no_bids))[:5]

    yes_asks = sorted([[100 - p, q] for p, q in no_bids], key=lambda x: x[0])
    no_asks = sorted([[100 - p, q] for p, q in yes_bids], key=lambda x: x[0])

    return {
        "yes_bids": yes_bids,
        "yes_asks": yes_asks,
        "no_bids": no_bids,
        "no_asks": no_asks
    }


# ==========================================================
# MAIN
# ==========================================================

import time
import math


def run_snapshot():

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    private_key = load_private_key_from_env()


    now = datetime.datetime.now()
    date_val = now.date().isoformat()
    time_val = now.time().strftime("%H:%M:%S")

    rows_to_insert = []

    open_events = get_all_open_events(private_key)

    for event in open_events:

        event_ticker = event.get("event_ticker")
        markets = get_markets_for_event(event_ticker, private_key)

        t_values = sorted([
            int(m.get("ticker").split("-")[-1][1:])
            for m in markets
            if m.get("ticker").split("-")[-1].startswith("T")
        ])

        for market in markets:

            ticker = market.get("ticker")
            depth = get_market_depth(ticker, private_key)

            suffix = ticker.split("-")[-1]

            bucket_type = None
            lower = None
            upper = None
            is_tail = False

            if suffix.startswith("B"):
                bucket_type = "range"
                val = float(suffix[1:])
                lower = int(val)
                upper = lower + 1

            elif suffix.startswith("T") and len(t_values) == 2:
                is_tail = True
                val = int(suffix[1:])
                if val == t_values[0]:
                    bucket_type = "below"
                    upper = val - 1
                else:
                    bucket_type = "above"
                    lower = val + 1

            row = {
                "date": date_val,
                "time": time_val,
                "event": event_ticker,
                "market": ticker,
                "bucket_type": bucket_type,
                "temp_lower_bound": lower,
                "temp_upper_bound": upper,
                "is_tail_bucket": is_tail
            }

            for side in ["yes_bids", "yes_asks", "no_bids", "no_asks"]:
                levels = depth.get(side, [])
                for i in range(5):
                    price_col = f"{side[:-1]}_{i+1}_price"
                    qty_col = f"{side[:-1]}_{i+1}_qty"
                    if i < len(levels):
                        row[price_col] = levels[i][0]
                        row[qty_col] = levels[i][1]
                    else:
                        row[price_col] = None
                        row[qty_col] = None

            rows_to_insert.append(row)

    if rows_to_insert:
        supabase.table(TABLE_NAME).insert(rows_to_insert).execute()
        print(f"[{datetime.datetime.now()}] Inserted {len(rows_to_insert)} rows.")
    else:
        print(f"[{datetime.datetime.now()}] No rows to insert.")


def sleep_until_next_5_min_mark():

    now = datetime.datetime.now()
    minute = now.minute
    second = now.second

    next_minute = math.ceil(minute / 5) * 5
    if next_minute == 60:
        next_minute = 0
        next_hour = now.hour + 1
    else:
        next_hour = now.hour

    next_run = now.replace(
        hour=next_hour,
        minute=next_minute,
        second=0,
        microsecond=0
    )

    if next_run <= now:
        next_run += datetime.timedelta(minutes=5)

    sleep_seconds = (next_run - now).total_seconds()

    print(f"Sleeping {int(sleep_seconds)} seconds until {next_run}")
    time.sleep(sleep_seconds)


if __name__ == "__main__":

    print("Starting 5-minute aligned loop...")

    while True:
        sleep_until_next_5_min_mark()

        try:
            run_snapshot()
        except Exception as e:
            print("Error occurred:", e)
