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
os.environ["TZ"] = "Asia/Kolkata"
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
import redis
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

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "").strip()

SPREAD_MAX, EMA_TAU        = 0.0015, 1.0
RATE_GLOBAL, RATE_SYMBOL   = 20, 5
CD                         = {"MOMENTUM": 300, "MEAN_REVERSION": 45, "GAP_FADE": 180}
HWM, LAT_WARN_MS           = 10000, 100
MUTE_CACHE_TTL             = 1.0
RVOL_WARMUP_S              = 300
PEAK_HALFLIFE_S            = 60.0
AUTO_RESTART_HOURS         = float(os.getenv("AUTO_RESTART_HOURS", "20"))

# Kill switches (heartbeats)
HB_TTL_S               = 3        # process-alive heartbeat TTL
DATA_HB_TTL_S          = 5        # data-flowing heartbeat TTL (kill switch #1)
HB_WRITE_INTERVAL_S    = 1

# Position tracking (ACK'd entries)
EXIT_SCORE_THRESHOLD   = 2.0      # |score| <= 2 -> fire exit alert
EXIT_COOLDOWN_S        = 300      # per-symbol exit alert cooldown
POSITION_TTL_S         = 6 * 3600 # spec: auto-cleanup stale tracked positions (6h+)
PENDING_TTL_S          = 24 * 3600# how long an un-ACK'd alert stays actionable in Redis


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


# ---------- heartbeat helper (used by Process A and B) ----------
def _start_hb_thread(name, r_sync, extra_check=None):
    """Background daemon thread refreshing alpha:hb:{name} every 1s with 3s TTL.
    Optional extra_check() -> (key, alive_bool); if alive, writes alpha:hb:{key} with DATA TTL."""
    def _loop():
        while True:
            try:
                r_sync.setex(f"alpha:hb:{name}", HB_TTL_S, "1")
                if extra_check:
                    k, alive = extra_check()
                    if alive: r_sync.setex(f"alpha:hb:{k}", DATA_HB_TTL_S, "1")
            except Exception:
                pass
            time.sleep(HB_WRITE_INTERVAL_S)
    threading.Thread(target=_loop, daemon=True, name=f"hb-{name}").start()


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
    pwd     = os.getenv("ANGEL_PASSWORD")
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

    if AUTO_RESTART_HOURS > 0:
        def _self_exit():
            print(f"[A] JWT nearing expiry ({AUTO_RESTART_HOURS}h up), exiting for restart", flush=True)
            os._exit(2)
        t = threading.Timer(AUTO_RESTART_HOURS * 3600, _self_exit); t.daemon = True; t.start()

    token2sym = _resolve_nse_tokens(UNIVERSE)
    tokens    = list(token2sym.keys())

    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB); pub.setsockopt(zmq.SNDHWM, HWM); pub.bind(ZMQ_TICKS)
    print(f"[A] Data Factory up, {len(tokens)} tokens mode=3", flush=True)

    # Heartbeats: kill switch #1 (data flow) + process-alive.
    r_sync = redis.from_url(REDIS_URL, decode_responses=True)
    last_tick = {"ts": 0.0}
    def _data_check(): return ("data", (time.time() - last_tick["ts"]) < DATA_HB_TTL_S)
    _start_hb_thread("A", r_sync, extra_check=_data_check)

    ws = SmartWebSocketV2(auth, api_key, client, feed)

    def on_data(_ws, m):
        sym = token2sym.get(str(m.get("token")))
        if not sym: return
        last_tick["ts"] = time.time()
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
        except zmq.Again: pass

    ws.on_data  = on_data
    ws.on_open  = lambda _w: ws.subscribe("adl", 3, [{"exchangeType": 1, "tokens": tokens}])
    ws.on_error = lambda _w, e: print(f"[A] WS error: {e}", flush=True)
    ws.on_close = lambda _w: print("[A] WS closed (SDK will reconnect)", flush=True)
    ws.connect()


# ---------- Process B: Alpha Engine ----------
@dataclass
class SymbolState:
    vol_60:      WindowSum = field(default_factory=lambda: WindowSum(60))
    vol_20m:     WindowSum = field(default_factory=lambda: WindowSum(1200))
    aggr_signed: WindowSum = field(default_factory=lambda: WindowSum(30))  # net trade-classified (Y1,Y2)
    aggr_abs:    WindowSum = field(default_factory=lambda: WindowSum(30))  # gross trade-classified
    ltp_30s:     deque     = field(default_factory=deque)
    last_vol:  int   = -1
    last_ltp:  float = 0.0                                                  # for tick-rule fallback
    ema_score: float = 0.0
    last_ts:   float = 0.0
    peak:      float = 0.0


# --- 9 calculators, all amortized O(1) per tick ---
def _c1_vwap_distance(_st, tick):
    if not tick["vwap"] or not tick["ltp"]: return 0.0
    dist = (tick["ltp"] - tick["vwap"]) / tick["vwap"] * 100
    return _clip(dist * 5.0, -2.0, 2.0)

def _c2_rvol(st, _tick):
    if st.vol_20m.span_s() < RVOL_WARMUP_S: return 1.0
    baseline_sum = max(st.vol_20m.s - st.vol_60.s, 0.0)
    baseline_per_min = baseline_sum / 19.0
    return st.vol_60.s / max(baseline_per_min, 1.0)

def _c3_agg_buy(st, _tick):
    """Rolling 30s net aggressive buy/sell (Y2 fix). Uses Lee-Ready + tick-rule
    classified volume; noise dampened vs single-tick classification."""
    total = st.aggr_abs.s
    if total < 100: return 0.0
    return _clip(st.aggr_signed.s / total * 2.0, -2.0, 2.0)

def _c4_absorption(st, tick):
    """Trade-classified absorption (Y1 fix):
       +1.5 : heavy net SELL volume + flat LTP -> sellers absorbed (BULLISH)
       -1.5 : heavy net BUY  volume + flat LTP -> buyers absorbed (BEARISH trap)"""
    if len(st.ltp_30s) < 5 or not tick["ltp"]: return 0.0
    lo = min(l for _, l in st.ltp_30s); hi = max(l for _, l in st.ltp_30s)
    if (hi - lo) / tick["ltp"] > 0.001: return 0.0
    if st.aggr_signed.span_s() < 10: return 0.0
    total = st.aggr_abs.s
    if total < 100: return 0.0
    ratio = st.aggr_signed.s / total
    if ratio < -0.4: return  1.5
    if ratio >  0.4: return -1.5
    return 0.0

def _c5_imbalance(_st, tick):
    """L5 book imbalance (Angel provides total across 5 depth levels)."""
    b, a = tick["buy_qty"], tick["sell_qty"]
    return _clip((b - a) / (b + a) * 2, -2.0, 2.0) if (b + a) else 0.0

def _c6_spread(_st, tick):
    return (tick["ask"] - tick["bid"]) / tick["ltp"] if tick["ltp"] and tick["bid"] and tick["ask"] else 1.0

def _c7_gap(_st, tick):
    t = time.localtime(tick["ts"])
    if not (t.tm_hour == 9 and 15 <= t.tm_min <= 30) or not tick["prev_close"]: return 0.0
    return _clip(-((tick["open"] - tick["prev_close"]) / tick["prev_close"]) * 100, -3.0, 3.0)

def _c8_session_weight(_st, tick):
    t = time.localtime(tick["ts"]); h, m = t.tm_hour, t.tm_min
    if h == 9 and m < 45: return 1.2
    if (h == 14 and m >= 45) or (h == 15 and m <= 30): return 1.1
    return 0.8


def feature_score(st, tick):
    rvol = _c2_rvol(st, tick)
    if _c6_spread(st, tick) > SPREAD_MAX: return 0.0, rvol
    vwap = _c1_vwap_distance(st, tick)
    agg  = _c3_agg_buy(st, tick)
    absb = _c4_absorption(st, tick)
    imb  = _c5_imbalance(st, tick)
    gap  = _c7_gap(st, tick)
    sw   = _c8_session_weight(st, tick)
    hidden = (max(agg, absb) + 0.4 * min(agg, absb)) if agg * absb >= 0 else (agg + absb)
    return _clip(sw * (2.0 * vwap + 1.5 * hidden + 1.5 * imb + 1.0 * gap), -10.0, 10.0), rvol


def _classify(tick, last_ltp):
    """Lee-Ready with tick-rule fallback. Returns +1 (aggressive buy), -1 (aggressive sell), 0."""
    mid = (tick["bid"] + tick["ask"]) / 2 if tick["bid"] and tick["ask"] else tick["ltp"]
    if tick["ltp"] > mid: return +1
    if tick["ltp"] < mid: return -1
    if last_ltp and tick["ltp"] > last_ltp: return +1
    if last_ltp and tick["ltp"] < last_ltp: return -1
    return 0


def _update_windows(st, tick):
    ts = tick["ts"]
    if st.last_vol < 0:
        st.last_vol = tick["vol"]; dvol = 0
    else:
        dvol = max(0, tick["vol"] - st.last_vol); st.last_vol = tick["vol"]
    st.vol_60.add(ts, dvol); st.vol_20m.add(ts, dvol)

    # Trade classification for aggression + absorption (Y1 + Y2 shared infrastructure)
    cls = _classify(tick, st.last_ltp)
    st.last_ltp = tick["ltp"]
    if cls != 0 and dvol > 0:
        st.aggr_signed.add(ts, cls * dvol)
        st.aggr_abs.add(ts, dvol)

    st.ltp_30s.append((ts, tick["ltp"]))
    cut = ts - 30
    while st.ltp_30s and st.ltp_30s[0][0] < cut: st.ltp_30s.popleft()


def alpha_engine():
    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.setsockopt(zmq.RCVHWM, HWM); sub.setsockopt(zmq.SUBSCRIBE, b""); sub.connect(ZMQ_TICKS)
    pub = ctx.socket(zmq.PUB); pub.setsockopt(zmq.SNDHWM, HWM); pub.bind(ZMQ_SCORES)
    r_sync = redis.from_url(REDIS_URL, decode_responses=True)
    _start_hb_thread("B", r_sync)                              # kill switch #2
    state: dict[str, SymbolState] = {}
    tick_count, last_stats = 0, time.time()
    print("[B] Alpha Engine up", flush=True)
    while True:
        sym_b, payload = sub.recv_multipart()
        tick = json.loads(payload)
        lat_ms = (time.time() - tick["ts"]) * 1000
        sym = tick["sym"]
        st = state.get(sym)
        if st is None: st = state[sym] = SymbolState()
        _update_windows(st, tick)
        raw, rvol = feature_score(st, tick)

        if st.last_ts == 0:
            st.ema_score = raw; st.peak = abs(raw)
        elif tick["ts"] > st.last_ts:
            dt = tick["ts"] - st.last_ts
            alpha = 1.0 - math.exp(-dt / EMA_TAU)
            st.ema_score = alpha * raw + (1 - alpha) * st.ema_score
            peak_decay = math.exp(-dt * math.log(2) / PEAK_HALFLIFE_S)
            st.peak = max(st.peak * peak_decay, abs(st.ema_score))
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
            def _last_ltp_of(s): return s.ltp_30s[-1][1] if s.ltp_30s else 0.0
            summary = "  ".join(f"{sm}={sst.ema_score:+.2f}(ltp={_last_ltp_of(sst):.1f})"
                                for sm, sst in top) or "-"
            rate = tick_count / max(now - last_stats, 0.001)
            print(f"[B] ticks={tick_count} ({rate:.0f}/s)  syms={len(state)}  top: {summary}", flush=True)
            tick_count, last_stats = 0, now


# ---------- Process C: Broadcaster + Telegram bot ----------
def mode_for(score, rvol):
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

async def _exit_cooldown_ok(r, sym):
    return bool(await r.set(f"alpha:exitcd:{sym}", "1", ex=EXIT_COOLDOWN_S, nx=True))


def _entry_alert_text(s, mode):
    side = "LONG" if s["score"] > 0 else "SHORT"
    sl = {"MOMENTUM": "1.5%", "MEAN_REVERSION": "0.5%", "GAP_FADE": "gap-based"}[mode]
    return (f"🔔 [{mode}] {s['sym']} {side}\n"
            f"score={s['score']:+.2f}  peak={s['peak']:.2f}  ltp={s['ltp']:.2f}\n"
            f"suggested SL: {sl}\nadvisory only — user executes manually")


def _exit_alert_text(sym, pos, cur_score, cur_ltp):
    side = "LONG" if pos["score_at_entry"] > 0 else "SHORT"
    entry = pos.get("ltp_entry", 0)
    pnl = ((cur_ltp - entry) / entry * 100) if entry else 0.0
    if side == "SHORT": pnl = -pnl
    return (f"⚠️ EXIT — {sym} ({pos['mode']} {side})\n"
            f"score decayed to {cur_score:+.2f} (entry {pos['score_at_entry']:+.2f})\n"
            f"ltp {cur_ltp:.2f} (entry {entry:.2f}, delta {pnl:+.2f}%)\n"
            f"consider closing position")


# --- Telegram bot (module-level so send helpers can reach app) ---
_TG = {"app": None, "kb_entry": None, "kb_exit": None}

async def _tg_init(r_async):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        print("[C] TELEGRAM_TOKEN/CHAT_ID missing — bot disabled, alerts to stdout", flush=True)
        return
    try:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        from telegram.ext import Application, CallbackQueryHandler
    except ImportError as e:
        print(f"[C] python-telegram-bot import failed: {e}", flush=True); return

    async def _on_button(update, context):
        q = update.callback_query
        try: await q.answer()
        except Exception: pass
        try:
            parts = q.data.split(":", 2)
            action = parts[0]; sym = parts[1]; mode = parts[2] if len(parts) > 2 else ""
            r = context.application.bot_data["redis"]

            if action == "ACK":
                info = await r.get(f"alpha:pending:{q.message.message_id}")
                if info:
                    await r.setex(f"alpha:pos:{sym}", POSITION_TTL_S, info)
                    await r.delete(f"alpha:pending:{q.message.message_id}")
                    await q.edit_message_reply_markup(reply_markup=None)
                    await q.edit_message_text(f"{q.message.text}\n\n✅ ACK'd — tracking for exit")
                else:
                    await q.edit_message_reply_markup(reply_markup=None)
                    await q.edit_message_text(f"{q.message.text}\n\n✅ ACK'd (details expired after 24h)")

            elif action == "SKIP":
                key = f"alpha:skip:{time.strftime('%Y%m%d')}"
                await r.lpush(key, json.dumps({"sym": sym, "mode": mode, "ts": time.time()}))
                await r.expire(key, 30 * 24 * 3600)
                await q.edit_message_reply_markup(reply_markup=None)
                await q.edit_message_text(f"{q.message.text}\n\n⏭️ SKIPPED (logged)")

            elif action == "DONE":
                await r.delete(f"alpha:pos:{sym}")
                await q.edit_message_reply_markup(reply_markup=None)
                await q.edit_message_text(f"{q.message.text}\n\n✔️ DONE — position closed")

            elif action == "HOLD":
                await r.setex(f"alpha:exitcd:{sym}", EXIT_COOLDOWN_S * 2, "1")
                await q.edit_message_reply_markup(reply_markup=None)
                await q.edit_message_text(f"{q.message.text}\n\n⏳ HOLDING — exit cooldown extended")
        except Exception as e:
            print(f"[C] button handler err: {e!r}", flush=True)

    try:
        app = Application.builder().token(TELEGRAM_TOKEN).build()
        app.add_handler(CallbackQueryHandler(_on_button))
        app.bot_data["redis"] = r_async
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        _TG["app"] = app
        _TG["kb_entry"] = lambda sym, mode: InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ ACK",  callback_data=f"ACK:{sym}:{mode}"),
            InlineKeyboardButton("⏭️ SKIP", callback_data=f"SKIP:{sym}:{mode}"),
        ]])
        _TG["kb_exit"] = lambda sym, mode: InlineKeyboardMarkup([[
            InlineKeyboardButton("✔️ DONE",      callback_data=f"DONE:{sym}:{mode}"),
            InlineKeyboardButton("⏳ HOLD MORE", callback_data=f"HOLD:{sym}:{mode}"),
        ]])
        # Send startup ping so user knows bot is live
        try: await app.bot.send_message(chat_id=TELEGRAM_CHAT, text="🟢 Alpha-Decay Lite bot online")
        except Exception as e: print(f"[C] startup ping failed: {e!r}", flush=True)
        print(f"[C] Telegram bot up (chat={TELEGRAM_CHAT})", flush=True)
    except Exception as e:
        print(f"[C] Telegram bot init failed: {e!r} — alerts to stdout only", flush=True)


async def _tg_shutdown():
    app = _TG["app"]
    if not app: return
    try:
        await app.updater.stop(); await app.stop(); await app.shutdown()
    except Exception as e:
        print(f"[C] TG shutdown err: {e!r}", flush=True)


async def _tg_send_entry(r_async, s, mode):
    text = _entry_alert_text(s, mode)
    app = _TG["app"]
    if app is None:
        print(f"[TG STUB ENTRY]\n{text}\n---", flush=True); return
    try:
        msg = await app.bot.send_message(chat_id=TELEGRAM_CHAT, text=text,
                                         reply_markup=_TG["kb_entry"](s["sym"], mode))
        # Persist pending -> Redis (survives restarts)
        pending = {"sym": s["sym"], "mode": mode, "score_at_entry": s["score"],
                   "peak": s["peak"], "ltp_entry": s["ltp"], "entry_ts": time.time()}
        await r_async.setex(f"alpha:pending:{msg.message_id}", PENDING_TTL_S, json.dumps(pending))
    except Exception as e:
        print(f"[C] TG entry send failed: {e!r}", flush=True)


async def _tg_send_exit(s, pos):
    text = _exit_alert_text(s["sym"], pos, s["score"], s["ltp"])
    app = _TG["app"]
    if app is None:
        print(f"[TG STUB EXIT]\n{text}\n---", flush=True); return
    try:
        await app.bot.send_message(chat_id=TELEGRAM_CHAT, text=text,
                                   reply_markup=_TG["kb_exit"](s["sym"], pos.get("mode", "")))
    except Exception as e:
        print(f"[C] TG exit send failed: {e!r}", flush=True)


async def _check_suppression(r):
    """Returns (suppressed, reason). Cached by caller to avoid hot-loop Redis+disk."""
    try:
        if bool(await r.get("alpha:mute")): return True, "muted"
        if os.path.exists(MUTE_FILE):       return True, "muted"
        if not await r.get("alpha:hb:B"):   return True, "engine_dead"
        if not await r.get("alpha:hb:data"): return True, "no_data"
    except Exception:
        return os.path.exists(MUTE_FILE), "muted"
    return False, ""


async def broadcaster_loop():
    ctx = zmq.asyncio.Context()
    sub = ctx.socket(zmq.SUB)
    sub.setsockopt(zmq.RCVHWM, HWM); sub.setsockopt(zmq.SUBSCRIBE, b""); sub.connect(ZMQ_SCORES)
    r = aioredis.from_url(REDIS_URL, decode_responses=True,
                          health_check_interval=30, socket_keepalive=True)
    await _tg_init(r)
    counts = {"scores": 0, "no_mode": 0, "muted": 0, "no_data": 0, "engine_dead": 0,
              "cooldown": 0, "rate": 0, "sent": 0, "exit_sent": 0, "redis_err": 0}
    last_stats = time.time()
    supp = {"suppressed": False, "reason": "", "ts": 0.0}
    print("[C] Broadcaster up", flush=True)
    try:
        while True:
            try:
                sym_b, payload = await sub.recv_multipart()
                s = json.loads(payload)
                counts["scores"] += 1

                # Refresh cached suppression (mute + heartbeats) at most 1x/sec
                now_wall = time.time()
                if now_wall - supp["ts"] > MUTE_CACHE_TTL:
                    s_supp, s_reason = await _check_suppression(r)
                    supp["suppressed"], supp["reason"], supp["ts"] = s_supp, s_reason, now_wall

                if supp["suppressed"]:
                    counts[supp["reason"]] = counts.get(supp["reason"], 0) + 1
                else:
                    # 1) Exit alerts for ACK'd positions when score decays
                    try:
                        tracked = await r.get(f"alpha:pos:{s['sym']}")
                    except Exception:
                        tracked = None; counts["redis_err"] += 1
                    if tracked and abs(s["score"]) <= EXIT_SCORE_THRESHOLD:
                        try:
                            if await _exit_cooldown_ok(r, s["sym"]):
                                await _tg_send_exit(s, json.loads(tracked))
                                counts["exit_sent"] += 1
                        except Exception as e:
                            counts["redis_err"] += 1
                            print(f"[C] exit err: {e!r}", flush=True)

                    # 2) Entry alerts
                    mode = mode_for(s["score"], s["rvol"])
                    if not mode:
                        counts["no_mode"] += 1
                    else:
                        try:
                            if not await _cooldown_ok(r, s["sym"], mode):
                                counts["cooldown"] += 1
                            elif not await _rate_ok(r, s["sym"]):
                                counts["rate"] += 1
                            else:
                                counts["sent"] += 1
                                await _tg_send_entry(r, s, mode)
                        except Exception as e:
                            counts["redis_err"] += 1
                            print(f"[C] entry err: {e!r}", flush=True)

                now = time.time()
                if now - last_stats >= 15:
                    tag = f" suppressed={supp['reason']}" if supp["suppressed"] else ""
                    print(f"[C] {counts}{tag}", flush=True)
                    counts = dict.fromkeys(counts, 0); last_stats = now
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[C] loop err: {e!r}", flush=True)
                await asyncio.sleep(0.1)
    finally:
        await _tg_shutdown()


def run_broadcaster(): asyncio.run(broadcaster_loop())


# ---------- launcher with watchdog + graceful shutdown ----------
def _shutdown(procs, reason):
    print(f"\n[launcher] shutdown: {reason}", flush=True)
    for p in procs:
        if p.is_alive(): p.terminate()
    deadline = time.time() + 5
    for p in procs:
        p.join(timeout=max(0.1, deadline - time.time()))
    for p in procs:
        if p.is_alive():
            print(f"[launcher] force-kill {p.name}", flush=True); p.kill(); p.join(timeout=2)


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
