#!/usr/bin/env python3
"""
diag_bidask.py -- verify whether Angel SmartAPI SDK mode 3 swaps
best_5_buy_data / best_5_sell_data at the output layer.

Run on the VPS during market hours:
    source .venv/bin/activate
    python diag_bidask.py

The script subscribes RELIANCE to mode 3, prints the first 5 depth
snapshots, and tells you:
  - if b5[0].price < s5[0].price  ->  labels are correct (bid < ask)
  - if b5[0].price > s5[0].price  ->  SDK is swapping (need ANGEL_BIDASK_SWAPPED=1)

If the diagnostic confirms a swap, set in .env:
    ANGEL_BIDASK_SWAPPED=1
and restart the main app.  The bid/ask assignment inside on_data will
compensate automatically.
"""
import os, sys, time, json
from dotenv import load_dotenv
load_dotenv()

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp

api_key = os.getenv("ANGEL_API_KEY")
client  = os.getenv("ANGEL_CLIENT_CODE")
pwd     = os.getenv("ANGEL_PASSWORD")
totp_s  = os.getenv("ANGEL_TOTP_SECRET")

smart = SmartConnect(api_key=api_key)
sess  = smart.generateSession(client, pwd, pyotp.TOTP(totp_s).now())
if not sess or not sess.get("data"):
    print(f"FATAL login: {sess}"); sys.exit(1)
auth, feed = sess["data"]["jwtToken"], smart.getfeedToken()
print(f"login OK, subscribing RELIANCE (token 2885) mode 3")

RELIANCE = "2885"          # NSE token
ticks_seen = [0]
observations = []

def on_data(_ws, m):
    ticks_seen[0] += 1
    b5 = m.get("best_5_buy_data")  or []
    s5 = m.get("best_5_sell_data") or []
    ltp = m.get("last_traded_price", 0) / 100.0
    if not b5 or not s5:
        print(f"[{ticks_seen[0]}] depth empty (LTP={ltp:.2f})")
    else:
        pb = b5[0].get("price", 0) / 100.0
        ps = s5[0].get("price", 0) / 100.0
        fb = b5[0].get("flag")
        fs = s5[0].get("flag")
        verdict = "BID<ASK OK" if pb < ps else ("BID>ASK SWAP!" if pb > ps else "EQUAL")
        observations.append(verdict)
        print(f"[{ticks_seen[0]}] LTP={ltp:.2f}  "
              f"b5[0]=(price={pb:.2f}, flag={fb})  "
              f"s5[0]=(price={ps:.2f}, flag={fs})  ->  {verdict}")
    if ticks_seen[0] >= 5:
        # Summarize + close
        swap_votes = sum(1 for v in observations if "SWAP" in v)
        ok_votes   = sum(1 for v in observations if "OK" in v)
        print("\n=== VERDICT ===")
        if swap_votes >= 3:
            print("✗ SDK swap CONFIRMED (b5 price > s5 price consistently)")
            print("  Fix: add to .env  ->  ANGEL_BIDASK_SWAPPED=1")
        elif ok_votes >= 3:
            print("✓ SDK labels correct (b5 price < s5 price)")
            print("  No compensation needed. Leave ANGEL_BIDASK_SWAPPED unset.")
        else:
            print("? Inconclusive (mostly equal spreads or empty depth).")
            print("  Retry during active market hours.")
        try: _ws.close_connection()
        except Exception: pass
        os._exit(0)

ws = SmartWebSocketV2(auth, api_key, client, feed)
ws.on_data  = on_data
ws.on_open  = lambda _w: ws.subscribe("diag", 3, [{"exchangeType": 1, "tokens": [RELIANCE]}])
ws.on_error = lambda _w, e: print(f"WS error: {e}")
ws.on_close = lambda _w: print("WS closed")
print("Connecting... (need market open for live data)")
try:
    ws.connect()
except KeyboardInterrupt:
    print("aborted"); sys.exit(1)
