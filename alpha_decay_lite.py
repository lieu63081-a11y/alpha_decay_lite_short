"""
Alpha-Decay Lite -- signal-only NSE cash equity alerting engine.
Real-time, per-tick pipeline:  Angel WS (mode 3)  ->  ZMQ  ->  9 calc + EMA  ->  Telegram
Advisory only.  No order placement.
"""
import os
import sys
import time
import signal
import threading

# Force IST for time.localtime() calls (fast C-level + correct TZ regardless of server).
os.environ["TZ"] = "Asia/Kolkata"                    # unconditional -- override any inherited TZ
if hasattr(time, "tzset"): time.tzset()

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
MUTE_CACHE_TTL             = 1.0             # seconds; keep small for emergency responsiveness
RVOL_WARMUP_S              = 300             # need 5+ min of history before RVOL is meaningful
PEAK_HALFLIFE_S            = 60.0            # peak amplitude half-life (time-based decay)
AUTO_RESTART_HOURS         = float(os.getenv("AUTO_RESTART_HOURS", "20"))  # < Angel JWT ~24-28h expiry


def _clip(x, lo, hi): return max(lo, min(hi, x))


# ---------- amortized-O(1) rolling window helpers ----------
class WindowSum:
    __slots__ = ("w", "q", "s")
    def __init__(self, window_s): self.w, self.q, self.s = window_s, deque(), 0.0
    def add(self, ts, v):
        self.q.append((ts, v)); self.s += v
        cut = ts - self.w
        while self.q and self.q[0][0] < cut: self.s -= self.q.popleft()[1]
    def span_s(self): return (self.q[-1][0] - self.q[0][0]) if len(self.q) >= 2 else 0.0


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
    try:
        totp_now = pyotp.TOTP(totp_s).now()
    except Exception as e:
        print(f"[A] FATAL invalid TOTP secret: {e}. ANGEL_TOTP_SECRET must be base32.")
        return
    sess = smart.generateSession(client, pwd, totp_now)
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

    # JWT auto-refresh: schedule self-exit before token expires (~24-28h).
    # Under systemd Restart=on-failure, this triggers a clean re-auth.
    if AUTO_RESTART_HOURS > 0:
        def _self_exit():
            print(f"[A] JWT nearing expiry ({AUTO_RESTART_HOURS}h up), exiting for restart", flush=True)
            os._exit(2)   # non-zero -> systemd restarts; parent watchdog also tears down peers
        t = threading.Timer(AUTO_RESTART_HOURS * 3600, _self_exit)
        t.daemon = True
        t.start()

    token2sym = _resolve_nse_tokens(UNIVERSE)
    tokens    = list(token2sym.keys())

    ctx = zmq.Context()                              # fresh per-process context
    pub = ctx.socket(zmq.PUB); pub.setsockopt(zmq.SNDHWM, HWM); pub.bind(ZMQ_TICKS)
    print(f"[A] Data Factory up, {len(tokens)} tokens mode=3", flush=True)

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
    ws.on_error = lambda _w, e: print(f"[A] WS error: {e}", flush=True)
    ws.on_close = lambda _w: print("[A] WS closed (SDK will reconnect)", flush=True)
    ws.connect()


# ---------- Process B: Alpha Engine ----------
@dataclass
class SymbolState:
    vol_60:  WindowSum = field(default_factory=lambda: WindowSum(60))
    vol_20m: WindowSum = field(default_factory=lambda: WindowSum(1200))
    ltp_30s: deque     = field(default_factory=deque)     # (ts, ltp) for absorption
    last_vol:  int   = -1                                  # -1 = uninitialized (first-tick baseline)
    ema_score: float = 0.0
    last_ts:   float = 0.0
    peak:      float = 0.0


# --- 9 calculators, all amortized O(1) per tick ---
def _c1_vwap_distance(_st, tick):
    if not tick["vwap"] or not tick["ltp"]: return 0.0
    dist = (tick["ltp"] - tick["vwap"]) / tick["vwap"] * 100
    return _clip(dist * 5.0, -2.0, 2.0)                   # 0.4% away -> ±2.0 (capped)

def _c2_rvol(st, _tick):
    """RVOL = 60s volume / avg-per-minute of PRIOR 19 min (self-excluded).
    Returns neutral 1.0 during warmup (< 5 min of history) to avoid startup spikes."""
    if st.vol_20m.span_s() < RVOL_WARMUP_S:
        return 1.0                                        # warmup: neutral rvol
    baseline_sum = max(st.vol_20m.s - st.vol_60.s, 0.0)   # exclude self (recent 60s)
    baseline_per_min = baseline_sum / 19.0
    return st.vol_60.s / max(baseline_per_min, 1.0)

def _c3_agg_buy(_st, tick):
    mid = (tick["bid"] + tick["ask"]) / 2 if tick["bid"] and tick["ask"] else tick["ltp"]
    return 1.0 if tick["ltp"] > mid else (-1.0 if tick["ltp"] < mid else 0.0)

def _c4_absorption(st, tick):
    """Symmetric absorption based on order-book depth ratio + flat LTP over 30s.
    NOTE: uses buy_qty/sell_qty from L5 book snapshot (not trade classification);
    a future improvement is trade-classified volume integration."""
    if len(st.ltp_30s) < 5 or not tick["ltp"]: return 0.0
    lo = min(l for _, l in st.ltp_30s); hi = max(l for _, l in st.ltp_30s)
    if (hi - lo) / tick["ltp"] > 0.001: return 0.0
    total = tick["buy_qty"] + tick["sell_qty"]
    if total < 1: return 0.0
    sell_ratio = tick["sell_qty"] / total
    if sell_ratio > 0.6: return  1.5
    if sell_ratio < 0.4: return -1.5
    return 0.0

def _c5_imbalance(_st, tick):
    """L5 book imbalance (total pending buy vs sell across 5 depth levels)."""
    b, a = tick["buy_qty"], tick["sell_qty"]
    return _clip((b - a) / (b + a) * 2, -2.0, 2.0) if (b + a) else 0.0

def _c6_spread(_st, tick):
    return (tick["ask"] - tick["bid"]) / tick["ltp"] if tick["ltp"] and tick["bid"] and tick["ask"] else 1.0

def _c7_gap(_st, tick):
    """Gap fade signal, IST 9:15:00 - 9:30:00 inclusive."""
    t = time.localtime(tick["ts"])
    if not (t.tm_hour == 9 and 15 <= t.tm_min <= 30) or not tick["prev_close"]: return 0.0
    return _clip(-((tick["open"] - tick["prev_close"]) / tick["prev_close"]) * 100, -3.0, 3.0)

def _c8_session_weight(_st, tick):
    """IST session-hour weighting. Market hours: 9:15 - 15:30."""
    t = time.localtime(tick["ts"])
    h, m = t.tm_hour, t.tm_min
    if h == 9 and m < 45: return 1.2                                          # OPEN_VOL (9:15-9:45)
    if (h == 14 and m >= 45) or (h == 15 and m <= 30): return 1.1             # CLOSE_HOUR (14:45-15:30)
    return 0.8                                                                 # MID_QUIET
# _c9 -> EMA smoother, applied inline in alpha_engine() loop.


def feature_score(st, tick):
    rvol = _c2_rvol(st, tick)                             # compute rvol regardless of spread gate
    if _c6_spread(st, tick) > SPREAD_MAX: return 0.0, rvol
    vwap = _c1_vwap_distance(st, tick)
    agg  = _c3_agg_buy(st, tick)
    absb = _c4_absorption(st, tick)
    imb  = _c5_imbalance(st, tick)
    gap  = _c7_gap(st, tick)
    sw   = _c8_session_weight(st, tick)
    hidden = (max(agg, absb) + 0.4 * min(agg, absb)) if agg * absb >= 0 else (agg + absb)
    return _clip(sw * (2.0 * vwap + 1.5 * hidden + 1.5 * imb + 1.0 * gap), -10.0, 10.0), rvol


def _update_windows(st, tick):
    ts = tick["ts"]
    if st.last_vol < 0:                                   # first-tick baseline
        st.last_vol = tick["vol"]
        dvol = 0
    else:
        dvol = max(0, tick["vol"] - st.last_vol)
        st.last_vol = tick["vol"]
    st.vol_60.add(ts, dvol); st.vol_20m.add(ts, dvol)
    st.ltp_30s.append((ts, tick["ltp"]))
    cut = ts - 30
    while st.ltp_30s and st.ltp_30s[0][0] < cut: st.ltp_30s.popleft()


def alpha_engine():
    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.setsockopt(zmq.RCVHWM, HWM); sub.setsockopt(zmq.SUBSCRIBE, b""); sub.connect(ZMQ_TICKS)
    pub = ctx.socket(zmq.PUB); pub.setsockopt(zmq.SNDHWM, HWM); pub.bind(ZMQ_SCORES)
    state: dict[str, SymbolState] = {}
    tick_count, last_stats = 0, time.time()
    print("[B] Alpha Engine up", flush=True)
    while True:
        sym_b, payload = sub.recv_multipart()
        tick = json.loads(payload)
        lat_ms = (time.time() - tick["ts"]) * 1000
        sym = tick["sym"]
        st = state.get(sym)
        if st is None:
            st = state[sym] = SymbolState()               # avoid per-tick SymbolState() alloc
        _update_windows(st, tick)
        raw, rvol = feature_score(st, tick)

        # --- calc 9: EMA smoother + time-based peak decay ---
        if st.last_ts == 0:                               # first tick: initialize
            st.ema_score = raw
            st.peak = abs(raw)
        elif tick["ts"] > st.last_ts:
            dt = tick["ts"] - st.last_ts
            alpha = 1.0 - math.exp(-dt / EMA_TAU)
            st.ema_score = alpha * raw + (1 - alpha) * st.ema_score
            peak_decay = math.exp(-dt * math.log(2) / PEAK_HALFLIFE_S)     # half-life in seconds
            st.peak = max(st.peak * peak_decay, abs(st.ema_score))
        # else: same-timestamp batched tick -> preserve prev ema_score and peak
        st.last_ts = tick["ts"]

        if lat_ms > LAT_WARN_MS: print(f"[B] high lat={lat_ms:.0f}ms sym={sym}", flush=True)
        pub.send_multipart([sym_b, json.dumps({
            "sym": sym, "score": st.ema_score, "peak": st.peak,
            "rvol": rvol, "ltp": tick["ltp"], "ts": tick["ts"],
        }).encode()])
        tick_count += 1
        now = time.time()
        if now - last_stats >= 10:
            top = sorted(state.items(), key=lambda kv: -abs(kv[1].ema_score))[:5]
            def _last_ltp(s): return s.ltp_30s[-1][1] if s.ltp_30s else 0.0
            summary = "  ".join(f"{sm}={sst.ema_score:+.2f}(ltp={_last_ltp(sst):.1f})"
                                for sm, sst in top) or "-"
            rate = tick_count / max(now - last_stats, 0.001)
            print(f"[B] ticks={tick_count} ({rate:.0f}/s)  syms={len(state)}  top: {summary}", flush=True)
            tick_count, last_stats = 0, now


# ---------- Process C: Broadcaster (fully async) ----------
def mode_for(score, rvol):
    """Modes:
       MOMENTUM       : |score| >= 8 and rvol >= 1.5
       MEAN_REVERSION : 3 <= |score| < 8 and rvol < 2.0
       else           : no signal (gray area, e.g. high score + low vol -> future FAKE_BREAKOUT)."""
    abs_s = abs(score)
    if abs_s >= 8   and rvol >= 1.5: return "MOMENTUM"
    if 3 <= abs_s < 8 and rvol < 2.0: return "MEAN_REVERSION"
    return None


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
    print(f"[TG]\n{text}\n---", flush=True)


async def broadcaster_loop():
    ctx = zmq.asyncio.Context()
    sub = ctx.socket(zmq.SUB)
    sub.setsockopt(zmq.RCVHWM, HWM); sub.setsockopt(zmq.SUBSCRIBE, b""); sub.connect(ZMQ_SCORES)
    r = aioredis.from_url(REDIS_URL, decode_responses=True,
                          health_check_interval=30, socket_keepalive=True)
    counts = {"scores": 0, "no_mode": 0, "muted": 0, "cooldown": 0, "rate": 0, "sent": 0, "redis_err": 0}
    last_stats = time.time()
    mute_state = {"muted": False, "ts": 0.0}
    print("[C] Broadcaster up", flush=True)
    while True:
        try:
            sym_b, payload = await sub.recv_multipart()
            s = json.loads(payload)
            counts["scores"] += 1
            mode = mode_for(s["score"], s["rvol"])
            if not mode:
                counts["no_mode"] += 1
            else:
                now_wall = time.time()
                if now_wall - mute_state["ts"] > MUTE_CACHE_TTL:
                    try:
                        mute_state["muted"] = bool(await r.get("alpha:mute")) or os.path.exists(MUTE_FILE)
                    except Exception as e:
                        counts["redis_err"] += 1
                        mute_state["muted"] = os.path.exists(MUTE_FILE)  # fall back to disk-only
                        print(f"[C] Redis err on mute check: {e}", flush=True)
                    mute_state["ts"] = now_wall
                if mute_state["muted"]:
                    counts["muted"] += 1
                else:
                    try:
                        if not await _cooldown_ok(r, s["sym"], mode):
                            counts["cooldown"] += 1
                        elif not await _rate_ok(r, s["sym"]):
                            counts["rate"] += 1
                        else:
                            counts["sent"] += 1
                            await send_telegram(build_alert(s, mode))
                    except Exception as e:
                        counts["redis_err"] += 1
                        print(f"[C] Redis err (skipping alert): {e}", flush=True)
            now = time.time()
            if now - last_stats >= 15:
                print(f"[C] {counts}", flush=True)
                counts = dict.fromkeys(counts, 0); last_stats = now
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[C] loop error: {e!r}", flush=True)
            await asyncio.sleep(0.1)          # avoid tight error loop


def run_broadcaster(): asyncio.run(broadcaster_loop())


# ---------- launcher with watchdog + graceful shutdown ----------
def _shutdown(procs, reason):
    print(f"\n[launcher] shutdown: {reason}", flush=True)
    for p in procs:
        if p.is_alive(): p.terminate()
    deadline = time.time() + 5
    for p in procs:
        remaining = max(0.1, deadline - time.time())
        p.join(timeout=remaining)
    for p in procs:
        if p.is_alive():
            print(f"[launcher] force-kill {p.name} (didn't respond to SIGTERM)", flush=True)
            p.kill()
            p.join(timeout=2)


if __name__ == "__main__":
    procs = [Process(target=data_factory,    name="A-DataFactory"),
             Process(target=alpha_engine,    name="B-AlphaEngine"),
             Process(target=run_broadcaster, name="C-Broadcaster")]
    for p in procs: p.start()

    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())

    try:
        while not stop.is_set():
            dead = [p for p in procs if not p.is_alive()]
            if dead:
                _shutdown(procs, f"process died: {[p.name for p in dead]} (exit={dead[0].exitcode})")
                sys.exit(1)
            time.sleep(1)
        _shutdown(procs, "signal received")
    except KeyboardInterrupt:
        _shutdown(procs, "KeyboardInterrupt")
