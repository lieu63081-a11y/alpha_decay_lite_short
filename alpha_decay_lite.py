"""
Alpha-Decay Lite -- signal-only NSE cash equity alerting engine.
Real-time, per-tick pipeline:  Angel WS (mode 3)  ->  ZMQ  ->  9 calc + EMA  ->  Telegram
Advisory only.  No order placement.
"""
import os
import time
import json
import math
import asyncio
import requests
from collections import deque
from dataclasses import dataclass, field
from multiprocessing import Process

import zmq
import zmq.asyncio
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

# ---------- config ----------
ZMQ_TICKS   = "ipc:///tmp/alpha_ticks.ipc"
ZMQ_SCORES  = "ipc:///tmp/alpha_scores.ipc"
REDIS_URL   = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MUTE_FILE   = os.getenv("MUTE_FILE", "/tmp/alpha_mute.flag")
UNIVERSE    = os.getenv("UNIVERSE", "RELIANCE,TCS,HDFCBANK").split(",")
SCRIP_URL   = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

SPREAD_MAX, EMA_TAU        = 0.0015, 1.0
RATE_GLOBAL, RATE_SYMBOL   = 20, 5
CD                         = {"MOMENTUM": 300, "MEAN_REVERSION": 45, "GAP_FADE": 180}
HWM, LAT_WARN_MS           = 10000, 100


def _clip(x, lo, hi): return max(lo, min(hi, x))


# ---------- amortized-O(1) rolling window helpers ----------
class WindowSum:
    __slots__ = ("w", "q", "s")
    def __init__(self, window_s): self.w, self.q, self.s = window_s, deque(), 0.0
    def add(self, ts, v):
        self.q.append((ts, v)); self.s += v
        cut = ts - self.w
        while self.q and self.q[0][0] < cut: self.s -= self.q.popleft()[1]

class WindowFirst:
    __slots__ = ("w", "q")
    def __init__(self, window_s): self.w, self.q = window_s, deque()
    def add(self, ts, v):
        self.q.append((ts, v))
        while len(self.q) > 1 and self.q[0][0] < ts - self.w: self.q.popleft()
    def first(self): return self.q[0][1] if self.q else None


# ---------- Process A: Data Factory (Angel SmartWebSocketV2 mode 3) ----------
def _resolve_nse_tokens(symbols):
    t0 = time.time()
    print(f"[A] Downloading scrip master (~4 MB)...", flush=True)
    master = requests.get(SCRIP_URL, timeout=60).json()
    nse = {r["symbol"].replace("-EQ", ""): r["token"]
           for r in master if r.get("exch_seg") == "NSE" and r.get("symbol", "").endswith("-EQ")}
    resolved = {nse[s]: s for s in symbols if s in nse}
    missing = [s for s in symbols if s not in nse]
    print(f"[A] Scrip master loaded in {time.time()-t0:.1f}s, resolved {len(resolved)}/{len(symbols)} symbols", flush=True)
    if missing: print(f"[A] Symbols NOT found: {missing}", flush=True)
    return resolved


def data_factory():
    from SmartApi import SmartConnect
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
    import pyotp

    api_key = os.getenv("ANGEL_API_KEY")
    client  = os.getenv("ANGEL_CLIENT_CODE")
    pwd     = os.getenv("ANGEL_PASSWORD")           # 4-digit MPIN, not web password
    totp_s  = os.getenv("ANGEL_TOTP_SECRET")

    missing = [k for k, v in {"ANGEL_API_KEY": api_key, "ANGEL_CLIENT_CODE": client,
                              "ANGEL_PASSWORD": pwd, "ANGEL_TOTP_SECRET": totp_s}.items() if not v]
    if missing:
        print(f"[A] FATAL missing env vars: {missing}. Fill .env and restart.")
        return

    print("[A] Logging in to Angel...", flush=True)
    smart = SmartConnect(api_key=api_key)
    sess  = smart.generateSession(client, pwd, pyotp.TOTP(totp_s).now())
    if not sess or not sess.get("data"):
        msg = (sess or {}).get("message", "unknown")
        print(f"[A] FATAL Angel login failed: {msg}")
        print("     Common fixes:")
        print("       - ANGEL_PASSWORD must be your 4-digit MPIN (not web login password)")
        print("       - ANGEL_TOTP_SECRET must be the full base32 secret (not 6-digit code)")
        print("       - Ensure system time is NTP-synced: `timedatectl`")
        return
    auth, feed = sess["data"]["jwtToken"], smart.getfeedToken()
    print(f"[A] Login OK, client={client}", flush=True)

    token2sym = _resolve_nse_tokens(UNIVERSE)
    tokens    = list(token2sym.keys())

    ctx = zmq.Context.instance()
    pub = ctx.socket(zmq.PUB); pub.setsockopt(zmq.SNDHWM, HWM); pub.bind(ZMQ_TICKS)
    print(f"[A] Data Factory up, {len(tokens)} tokens mode=3")

    ws = SmartWebSocketV2(auth, api_key, client, feed)

    def on_data(_ws, m):
        sym = token2sym.get(str(m.get("token")))
        if not sym: return
        b5, s5 = m.get("best_5_buy_data") or [], m.get("best_5_sell_data") or []
        tick = {
            "sym": sym, "ts": time.time(),
            "ltp":  m.get("last_traded_price", 0) / 100.0,
            "bid":  (b5[0]["price"] / 100.0) if b5 else 0.0,
            "ask":  (s5[0]["price"] / 100.0) if s5 else 0.0,
            "vol":  m.get("volume_trade_for_the_day", 0),
            "vwap": m.get("average_traded_price", 0) / 100.0,
            "buy_qty":    m.get("total_buy_quantity", 0),
            "sell_qty":   m.get("total_sell_quantity", 0),
            "open":       m.get("open_price_of_the_day", 0) / 100.0,
            "prev_close": m.get("closed_price", 0) / 100.0,
        }
        try: pub.send_multipart([sym.encode(), json.dumps(tick).encode()], zmq.DONTWAIT)
        except zmq.Again: pass    # drop under backpressure (PUB HWM)

    ws.on_data  = on_data
    ws.on_open  = lambda _w: ws.subscribe("adl", 3, [{"exchangeType": 1, "tokens": tokens}])
    ws.on_error = lambda _w, e: print(f"[A] WS error: {e}")
    ws.on_close = lambda _w: print("[A] WS closed (SDK will reconnect)")
    ws.connect()


# ---------- Process B: Alpha Engine ----------
@dataclass
class SymbolState:
    vol_60:  WindowSum   = field(default_factory=lambda: WindowSum(60))
    vol_20m: WindowSum   = field(default_factory=lambda: WindowSum(1200))
    vwap_60: WindowFirst = field(default_factory=lambda: WindowFirst(60))
    ltp_30s: deque       = field(default_factory=deque)    # (ts, ltp) for absorb
    last_vol:  int   = 0
    ema_score: float = 0.0
    last_ts:   float = 0.0
    peak:      float = 0.0


# --- 9 calculators, all amortized O(1) per tick ---
def _c1_vwap_slope(st, tick):
    first = st.vwap_60.first()
    if not first: return 0.0
    return _clip((tick["vwap"] - first) / first * 500, -2.0, 2.0)

def _c2_rvol(st, _tick):
    return st.vol_60.s / max(st.vol_20m.s / 20.0, 1.0)

def _c3_agg_buy(_st, tick):
    mid = (tick["bid"] + tick["ask"]) / 2 if tick["bid"] and tick["ask"] else tick["ltp"]
    return 1.0 if tick["ltp"] > mid else (-1.0 if tick["ltp"] < mid else 0.0)

def _c4_sell_absorb(st, tick):
    if len(st.ltp_30s) < 5 or tick["ltp"] == 0: return 0.0
    lo = min(l for _, l in st.ltp_30s); hi = max(l for _, l in st.ltp_30s)
    pressure = tick["sell_qty"] / max(tick["buy_qty"] + tick["sell_qty"], 1)
    return 1.5 if ((hi - lo) / tick["ltp"] < 0.001 and pressure > 0.6) else 0.0

def _c5_imbalance(_st, tick):
    b, a = tick["buy_qty"], tick["sell_qty"]
    return _clip((b - a) / (b + a) * 2, -2.0, 2.0) if (b + a) else 0.0

def _c6_spread(_st, tick):
    return (tick["ask"] - tick["bid"]) / tick["ltp"] if tick["ltp"] and tick["bid"] and tick["ask"] else 1.0

def _c7_gap(_st, tick):
    t = time.localtime(tick["ts"])
    if not (t.tm_hour == 9 and 15 <= t.tm_min < 30) or not tick["prev_close"]: return 0.0
    return _clip(-((tick["open"] - tick["prev_close"]) / tick["prev_close"]) * 100, -3.0, 3.0)

def _c8_session_weight(_st, tick):
    t = time.localtime(tick["ts"])
    if t.tm_hour == 9 and t.tm_min < 45: return 1.2                            # OPEN_VOL
    if t.tm_hour >= 15 or (t.tm_hour == 14 and t.tm_min >= 45): return 1.1     # CLOSE_HOUR
    return 0.8                                                                  # MID_QUIET
# _c9 -> EMA smoother, applied inline in alpha_engine() loop.


def feature_score(st, tick):
    if _c6_spread(st, tick) > SPREAD_MAX: return 0.0, 1.0
    vwap = _c1_vwap_slope(st, tick)
    rvol = _c2_rvol(st, tick)
    agg  = _c3_agg_buy(st, tick)
    absb = _c4_sell_absorb(st, tick)
    imb  = _c5_imbalance(st, tick)
    gap  = _c7_gap(st, tick)
    sw   = _c8_session_weight(st, tick)
    hidden = (max(agg, absb) + 0.4 * min(agg, absb)) if agg * absb >= 0 else (agg + absb)
    return _clip(sw * (2.0 * vwap + 1.5 * hidden + 1.5 * imb + 1.0 * gap), -10.0, 10.0), rvol


def _update_windows(st, tick):
    dvol = max(0, tick["vol"] - st.last_vol); st.last_vol = tick["vol"]
    ts = tick["ts"]
    st.vol_60.add(ts, dvol); st.vol_20m.add(ts, dvol)
    if tick["vwap"] > 0: st.vwap_60.add(ts, tick["vwap"])
    st.ltp_30s.append((ts, tick["ltp"]))
    cut = ts - 30
    while st.ltp_30s and st.ltp_30s[0][0] < cut: st.ltp_30s.popleft()


def alpha_engine():
    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB)
    sub.setsockopt(zmq.RCVHWM, HWM); sub.setsockopt(zmq.SUBSCRIBE, b""); sub.connect(ZMQ_TICKS)
    pub = ctx.socket(zmq.PUB); pub.setsockopt(zmq.SNDHWM, HWM); pub.bind(ZMQ_SCORES)
    state: dict[str, SymbolState] = {}
    print("[B] Alpha Engine up")
    while True:
        sym_b, payload = sub.recv_multipart()
        tick = json.loads(payload)
        lat_ms = (time.time() - tick["ts"]) * 1000
        st = state.setdefault(tick["sym"], SymbolState())
        _update_windows(st, tick)
        raw, rvol = feature_score(st, tick)
        dt = (tick["ts"] - st.last_ts) if st.last_ts else 0.0
        alpha = 1.0 - math.exp(-dt / EMA_TAU) if dt > 0 else 1.0                # calc 9
        st.ema_score = alpha * raw + (1 - alpha) * st.ema_score
        st.last_ts = tick["ts"]
        st.peak    = max(st.peak * 0.995, abs(st.ema_score))
        if lat_ms > LAT_WARN_MS: print(f"[B] high lat={lat_ms:.0f}ms sym={tick['sym']}")
        pub.send_multipart([sym_b, json.dumps({
            "sym": tick["sym"], "score": st.ema_score, "peak": st.peak,
            "rvol": rvol, "ltp": tick["ltp"], "ts": tick["ts"],
        }).encode()])


# ---------- Process C: Broadcaster (fully async) ----------
def mode_for(score, rvol):
    if abs(score) >= 8 and rvol > 2.0:     return "MOMENTUM"
    if 3 <= abs(score) < 8 and rvol < 1.5: return "MEAN_REVERSION"
    return None


async def _is_muted(r):
    return bool(await r.get("alpha:mute")) or os.path.exists(MUTE_FILE)

async def _rate_ok(r, sym):
    hour, day = int(time.time() // 3600), time.strftime("%Y%m%d")
    gkey, skey = f"alpha:count:global:{hour}", f"alpha:count:{sym}:{day}"
    if int((await r.get(gkey)) or 0) >= RATE_GLOBAL: return False
    if int((await r.get(skey)) or 0) >= RATE_SYMBOL: return False
    p = r.pipeline(); p.incr(gkey); p.expire(gkey, 3700); p.incr(skey); p.expire(skey, 6*3600)
    await p.execute(); return True

async def _cooldown_ok(r, sym, mode):
    return bool(await r.set(f"alpha:cd:{sym}:{mode}", "1", ex=CD[mode], nx=True))


def build_alert(s, mode):
    side = "LONG" if s["score"] > 0 else "SHORT"
    sl = {"MOMENTUM": "1.5%", "MEAN_REVERSION": "0.5%", "GAP_FADE": "gap-based"}[mode]
    return (f"[{mode}] {s['sym']} {side}\nscore={s['score']:+.2f}  peak={s['peak']:.2f}  ltp={s['ltp']}\n"
            f"suggested SL: {sl}\nadvisory only -- user executes manually")


async def send_telegram(text):
    # TODO: python-telegram-bot Application.bot.send_message(chat_id, text, reply_markup=<ACK/SKIP>)
    print(f"[TG]\n{text}\n---")


async def broadcaster_loop():
    ctx = zmq.asyncio.Context.instance()
    sub = ctx.socket(zmq.SUB)
    sub.setsockopt(zmq.RCVHWM, HWM); sub.setsockopt(zmq.SUBSCRIBE, b""); sub.connect(ZMQ_SCORES)
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    print("[C] Broadcaster up")
    while True:
        sym_b, payload = await sub.recv_multipart()
        s = json.loads(payload)
        mode = mode_for(s["score"], s["rvol"])
        if not mode: continue
        if await _is_muted(r): continue
        if not await _cooldown_ok(r, s["sym"], mode): continue
        if not await _rate_ok(r, s["sym"]): continue
        await send_telegram(build_alert(s, mode))


def run_broadcaster(): asyncio.run(broadcaster_loop())


# ---------- launcher ----------
if __name__ == "__main__":
    procs = [Process(target=data_factory,    name="A-DataFactory"),
             Process(target=alpha_engine,    name="B-AlphaEngine"),
             Process(target=run_broadcaster, name="C-Broadcaster")]
    for p in procs: p.start()
    try:
        for p in procs: p.join()
    except KeyboardInterrupt:
        for p in procs: p.terminate()
