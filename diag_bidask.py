#!/usr/bin/env python3
"""
diag_bidask.py -- verify whether the Angel SmartAPI SDK's mode-3
(SNAP_QUOTE) response swaps best_5_buy_data / best_5_sell_data at the
output layer.

Run on the VPS during market hours:
    source .venv/bin/activate
    python diag_bidask.py

The script subscribes RELIANCE to mode 3, prints the first 5 depth
snapshots, and tells you:
  - if best_bid_price < best_ask_price  ->  labels are correct
  - if best_bid_price > best_ask_price  ->  SDK is swapping them
                                             (set ANGEL_BIDASK_SWAPPED=1)

If the diagnostic confirms a swap, set in .env:
    ANGEL_BIDASK_SWAPPED=1
and restart the main app. The bid/ask assignment inside
alpha_decay_lite.py's on_tick_received() will compensate automatically.
"""
import os
import sys
import time
import json

from dotenv import load_dotenv
load_dotenv()

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp

angel_api_key      = os.getenv("ANGEL_API_KEY")
angel_client_code   = os.getenv("ANGEL_CLIENT_CODE")
angel_password       = os.getenv("ANGEL_PASSWORD")
angel_totp_secret    = os.getenv("ANGEL_TOTP_SECRET")

angel_connection = SmartConnect(api_key=angel_api_key)
login_session = angel_connection.generateSession(
    angel_client_code, angel_password, pyotp.TOTP(angel_totp_secret).now())
if not login_session or not login_session.get("data"):
    print(f"FATAL login: {login_session}")
    sys.exit(1)

auth_token = login_session["data"]["jwtToken"]
feed_token = angel_connection.getfeedToken()
print("login OK, subscribing RELIANCE (token 2885) mode 3")

RELIANCE_NSE_TOKEN = "2885"
ticks_seen_count = [0]           # boxed in a list so the closure below can mutate it
observed_verdicts = []


def on_tick_received(_websocket, market_data_message):
    ticks_seen_count[0] += 1

    best_5_buy_levels  = market_data_message.get("best_5_buy_data")  or []
    best_5_sell_levels = market_data_message.get("best_5_sell_data") or []
    last_traded_price = market_data_message.get("last_traded_price", 0) / 100.0

    if not best_5_buy_levels or not best_5_sell_levels:
        print(f"[{ticks_seen_count[0]}] depth empty (LTP={last_traded_price:.2f})")
    else:
        best_bid_price = best_5_buy_levels[0].get("price", 0) / 100.0
        best_ask_price = best_5_sell_levels[0].get("price", 0) / 100.0
        best_bid_flag  = best_5_buy_levels[0].get("flag")
        best_ask_flag  = best_5_sell_levels[0].get("flag")

        if best_bid_price < best_ask_price:
            verdict = "BID<ASK OK"
        elif best_bid_price > best_ask_price:
            verdict = "BID>ASK SWAP!"
        else:
            verdict = "EQUAL"
        observed_verdicts.append(verdict)

        print(f"[{ticks_seen_count[0]}] LTP={last_traded_price:.2f}  "
              f"best_bid=(price={best_bid_price:.2f}, flag={best_bid_flag})  "
              f"best_ask=(price={best_ask_price:.2f}, flag={best_ask_flag})  "
              f"->  {verdict}")

    if ticks_seen_count[0] >= 5:
        swap_vote_count = sum(1 for verdict in observed_verdicts if "SWAP" in verdict)
        ok_vote_count    = sum(1 for verdict in observed_verdicts if "OK" in verdict)

        print("\n=== VERDICT ===")
        if swap_vote_count >= 3:
            print("X SDK swap CONFIRMED (best_bid_price > best_ask_price consistently)")
            print("  Fix: add to .env  ->  ANGEL_BIDASK_SWAPPED=1")
        elif ok_vote_count >= 3:
            print("OK SDK labels correct (best_bid_price < best_ask_price)")
            print("  No compensation needed. Leave ANGEL_BIDASK_SWAPPED unset.")
        else:
            print("? Inconclusive (mostly equal spreads or empty depth).")
            print("  Retry during active market hours.")

        try:
            websocket_client.close_connection()
        except Exception:
            pass
        os._exit(0)


websocket_client = SmartWebSocketV2(auth_token, angel_api_key, angel_client_code, feed_token)
websocket_client.on_data  = on_tick_received
websocket_client.on_open  = lambda _websocket: websocket_client.subscribe(
    "diag", 3, [{"exchangeType": 1, "tokens": [RELIANCE_NSE_TOKEN]}])
websocket_client.on_error = lambda _websocket, error: print(f"WS error: {error}")
websocket_client.on_close = lambda _websocket: print("WS closed")

print("Connecting... (need market open for live data)")
try:
    websocket_client.connect()
except KeyboardInterrupt:
    print("aborted")
    sys.exit(1)
