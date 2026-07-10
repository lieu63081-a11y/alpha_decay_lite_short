"""
tick_store.py -- broker-agnostic NSE tick capture + REST query API.

Designed for cheap 1-vCPU / 2 GB VPS. Run one instance per broker on
separate servers. Same script; switch BROKER env var.

Storage: one SQLite file per day at $DATA_DIR/ticks_YYYYMMDD.db (WAL mode).
Query:   FastAPI on $API_PORT with endpoints /health /symbols /ticks /latest /ohlc.
"""
import os
import sys
import time
import signal
import sqlite3
import threading
from queue import Queue, Empty
from datetime import datetime, timedelta

os.environ["TZ"] = "Asia/Kolkata"
if hasattr(time, "tzset"): time.tzset()

from fastapi import FastAPI, Query, HTTPException
import uvicorn
from dotenv import load_dotenv

load_dotenv()

# ---------- config ----------
BROKER            = os.getenv("BROKER", "angel").lower()
DATA_DIR          = os.getenv("DATA_DIR", "./data")
UNIVERSE          = [s.strip() for s in os.getenv("UNIVERSE", "RELIANCE,TCS").split(",") if s.strip()]
API_HOST          = os.getenv("API_HOST", "0.0.0.0")
API_PORT          = int(os.getenv("API_PORT", "8080"))
FLUSH_INTERVAL_S  = float(os.getenv("FLUSH_INTERVAL_S", "5"))
FLUSH_BATCH_MAX   = int(os.getenv("FLUSH_BATCH_MAX", "1000"))
KEEP_DAYS         = int(os.getenv("KEEP_DAYS", "90"))

os.makedirs(DATA_DIR, exist_ok=True)

# ---------- SQLite helpers ----------
SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
    sym TEXT NOT NULL,
    ts INTEGER NOT NULL,           -- ms since epoch
    ltp REAL, bid REAL, ask REAL,
    vol INTEGER, vwap REAL,
    buy_qty INTEGER, sell_qty INTEGER,
    open REAL, prev_close REAL
);
CREATE INDEX IF NOT EXISTS idx_sym_ts ON ticks(sym, ts);
"""

def _db_path_for(ts):
    return os.path.join(DATA_DIR, f"ticks_{time.strftime('%Y%m%d', time.localtime(ts))}.db")

def _open_write(path):
    conn = sqlite3.connect(path, isolation_level=None)   # autocommit
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.executescript(SCHEMA)
    return conn

def _open_read(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)

# ---------- writer thread (batched inserts) ----------
q_ticks: Queue = Queue()
_stats  = {"received": 0, "written": 0, "flushes": 0, "errors": 0, "last_flush_ts": 0}
_shutdown = threading.Event()

def writer_loop():
    conn = None
    current_date = None
    buffer = []
    last_flush = time.time()
    print("[writer] up", flush=True)

    def _flush():
        nonlocal conn, current_date, last_flush
        if not buffer: return
        date_str = time.strftime("%Y%m%d", time.localtime(buffer[0]["ts"]))
        if date_str != current_date:
            if conn:
                try: conn.close()
                except Exception: pass
            conn = _open_write(_db_path_for(buffer[0]["ts"]))
            current_date = date_str
            print(f"[writer] active db: {_db_path_for(buffer[0]['ts'])}", flush=True)
        try:
            rows = [(t["sym"], int(t["ts"] * 1000),
                     t.get("ltp"), t.get("bid"), t.get("ask"),
                     t.get("vol"), t.get("vwap"),
                     t.get("buy_qty"), t.get("sell_qty"),
                     t.get("open"), t.get("prev_close"))
                    for t in buffer]
            conn.executemany("""INSERT INTO ticks
                (sym, ts, ltp, bid, ask, vol, vwap, buy_qty, sell_qty, open, prev_close)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""", rows)
            _stats["written"]  += len(rows)
            _stats["flushes"]  += 1
            _stats["last_flush_ts"] = time.time()
        except Exception as e:
            _stats["errors"] += 1
            print(f"[writer] flush err: {e!r}", flush=True)
        buffer.clear()
        last_flush = time.time()

    while not (_shutdown.is_set() and q_ticks.empty()):
        try:
            item = q_ticks.get(timeout=1.0)
            if item is None: break
            buffer.append(item)
            _stats["received"] += 1
        except Empty:
            pass
        now = time.time()
        if buffer and (len(buffer) >= FLUSH_BATCH_MAX or now - last_flush >= FLUSH_INTERVAL_S):
            _flush()
    _flush()                                                     # final flush on shutdown
    if conn:
        try: conn.close()
        except Exception: pass
    print("[writer] stopped", flush=True)


# ---------- cleanup thread (auto-delete old DBs) ----------
def cleanup_loop():
    while not _shutdown.is_set():
        try:
            cutoff = (datetime.now() - timedelta(days=KEEP_DAYS)).strftime("%Y%m%d")
            for f in os.listdir(DATA_DIR):
                if f.startswith("ticks_") and f.endswith(".db"):
                    date_part = f.replace("ticks_", "").replace(".db", "")
                    if date_part < cutoff:
                        os.remove(os.path.join(DATA_DIR, f))
                        # cleanup WAL/SHM sidecars too
                        for suffix in ("-wal", "-shm"):
                            side = os.path.join(DATA_DIR, f + suffix)
                            if os.path.exists(side): os.remove(side)
                        print(f"[cleanup] removed {f} (older than {KEEP_DAYS}d)", flush=True)
        except Exception as e:
            print(f"[cleanup] err: {e!r}", flush=True)
        for _ in range(3600):
            if _shutdown.is_set(): return
            time.sleep(1)


# ---------- Broker A: Angel One ----------
def angel_ingester():
    from SmartApi import SmartConnect
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
    import pyotp, requests

    api_key = os.getenv("ANGEL_API_KEY")
    client  = os.getenv("ANGEL_CLIENT_CODE")
    pwd     = os.getenv("ANGEL_PASSWORD")
    totp_s  = os.getenv("ANGEL_TOTP_SECRET")
    for k, v in [("ANGEL_API_KEY", api_key), ("ANGEL_CLIENT_CODE", client),
                 ("ANGEL_PASSWORD", pwd), ("ANGEL_TOTP_SECRET", totp_s)]:
        if not v: print(f"[angel] FATAL missing {k}"); return

    smart = SmartConnect(api_key=api_key)
    sess = smart.generateSession(client, pwd, pyotp.TOTP(totp_s).now())
    if not sess or not sess.get("data"):
        print(f"[angel] FATAL login: {sess}"); return
    auth = sess["data"]["jwtToken"]; feed = smart.getfeedToken()

    scrip_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    master = requests.get(scrip_url, timeout=60).json()
    nse = {r["symbol"].replace("-EQ", ""): r["token"]
           for r in master if r.get("exch_seg") == "NSE" and r.get("symbol", "").endswith("-EQ")}
    token2sym = {nse[s]: s for s in UNIVERSE if s in nse}
    tokens = list(token2sym.keys())
    swap = os.getenv("ANGEL_BIDASK_SWAPPED", "0") == "1"
    print(f"[angel] login OK, subscribing {len(tokens)}/{len(UNIVERSE)} symbols", flush=True)

    try:
        ws = SmartWebSocketV2(auth, api_key, client, feed,
                              max_retry_attempt=10, retry_strategy=1,
                              retry_delay=5, retry_multiplier=2, retry_duration=60)
    except TypeError:
        ws = SmartWebSocketV2(auth, api_key, client, feed)

    def on_data(_ws, m):
        sym = token2sym.get(str(m.get("token")))
        if not sym: return
        b5 = m.get("best_5_buy_data") or []
        s5 = m.get("best_5_sell_data") or []
        if swap: b5, s5 = s5, b5
        q_ticks.put({
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
        })

    ws.on_data  = on_data
    ws.on_open  = lambda _w: ws.subscribe("tickstore", 3, [{"exchangeType": 1, "tokens": tokens}])
    ws.on_error = lambda _w, e: print(f"[angel] WS error: {e}", flush=True)
    ws.on_close = lambda _w: print("[angel] WS closed", flush=True)
    ws.connect()


# ---------- Broker B: Upstox (template — fill in per Upstox v2 SDK docs) ----------
def upstox_ingester():
    """Upstox WebSocket ingester -- TEMPLATE.
    Install:  pip install upstox-python-sdk
    Docs:     https://upstox.com/developer/api-documentation/websocket
    Auth:     access token from OAuth flow, refreshed daily
    """
    # from upstox_client import Configuration, ApiClient
    # from upstox_client.feeder import MarketDataStreamerV3
    #
    # cfg = Configuration(); cfg.access_token = os.getenv("UPSTOX_ACCESS_TOKEN")
    # api_client = ApiClient(cfg)
    # instruments = [os.getenv(f"UPSTOX_INSTRUMENT_{s}", "") for s in UNIVERSE]  # NSE_EQ|INE... format
    # instruments = [i for i in instruments if i]
    # streamer = MarketDataStreamerV3(api_client, instruments, "full")
    #
    # def on_message(msg):
    #     # msg format depends on SDK version; extract fields:
    #     q_ticks.put({
    #         "sym": <symbol>, "ts": time.time(),
    #         "ltp": <ltp>, "bid": <bid>, "ask": <ask>,
    #         "vol": <vol>, "vwap": <vwap>,
    #         "buy_qty": <buy_qty>, "sell_qty": <sell_qty>,
    #         "open": <open>, "prev_close": <prev_close>,
    #     })
    # streamer.on("message", on_message)
    # streamer.connect()
    print("[upstox] TODO: fill in upstox_ingester() -- see comments in code", flush=True)
    while not _shutdown.is_set(): time.sleep(60)


BROKERS = {"angel": angel_ingester, "upstox": upstox_ingester}


# ---------- FastAPI ----------
app = FastAPI(title=f"Tick Store ({BROKER})",
              description="NSE tick capture + query API")

@app.get("/health")
async def health():
    return {
        "ok": True,
        "broker": BROKER,
        "universe": UNIVERSE,
        "queue_size": q_ticks.qsize(),
        "stats": _stats,
        "data_dir": DATA_DIR,
        "keep_days": KEEP_DAYS,
    }

@app.get("/dates")
async def dates():
    files = sorted([f for f in os.listdir(DATA_DIR)
                    if f.startswith("ticks_") and f.endswith(".db")])
    return {"dates": [f.replace("ticks_", "").replace(".db", "") for f in files]}

@app.get("/symbols")
async def symbols(date: str = Query(default=None, regex=r"^\d{8}$")):
    date = date or time.strftime("%Y%m%d")
    path = os.path.join(DATA_DIR, f"ticks_{date}.db")
    if not os.path.exists(path):
        return {"date": date, "symbols": []}
    conn = _open_read(path)
    try:
        rows = conn.execute("SELECT DISTINCT sym FROM ticks ORDER BY sym").fetchall()
    finally:
        conn.close()
    return {"date": date, "symbols": [r[0] for r in rows]}


def _parse_time_arg(date_str, t_str, name):
    parts = t_str.split(":")
    h, m = int(parts[0]), int(parts[1])
    s = int(parts[2]) if len(parts) > 2 else 0
    try:
        dt = datetime.strptime(f"{date_str} {h:02d}:{m:02d}:{s:02d}", "%Y%m%d %H:%M:%S")
        return int(dt.timestamp() * 1000)
    except Exception as e:
        raise HTTPException(400, f"bad {name}: {e}")


@app.get("/ticks")
async def ticks(
    sym: str,
    date: str = Query(default=None, regex=r"^\d{8}$"),
    start: str = Query(default=None, description="HH:MM or HH:MM:SS IST"),
    end:   str = Query(default=None, description="HH:MM or HH:MM:SS IST"),
    limit: int = Query(default=10000, ge=1, le=100000),
):
    date = date or time.strftime("%Y%m%d")
    path = os.path.join(DATA_DIR, f"ticks_{date}.db")
    if not os.path.exists(path):
        return {"date": date, "sym": sym, "count": 0, "ticks": []}
    q = "SELECT ts, ltp, bid, ask, vol, vwap, buy_qty, sell_qty FROM ticks WHERE sym = ?"
    args = [sym]
    if start:
        q += " AND ts >= ?"; args.append(_parse_time_arg(date, start, "start"))
    if end:
        q += " AND ts <= ?"; args.append(_parse_time_arg(date, end, "end"))
    q += " ORDER BY ts LIMIT ?"; args.append(limit)
    conn = _open_read(path)
    try:
        rows = conn.execute(q, args).fetchall()
    finally:
        conn.close()
    return {"date": date, "sym": sym, "count": len(rows), "ticks": [
        {"ts": r[0], "ltp": r[1], "bid": r[2], "ask": r[3],
         "vol": r[4], "vwap": r[5], "buy_qty": r[6], "sell_qty": r[7]}
        for r in rows
    ]}


@app.get("/latest")
async def latest(sym: str, n: int = Query(default=100, ge=1, le=10000)):
    date = time.strftime("%Y%m%d")
    path = os.path.join(DATA_DIR, f"ticks_{date}.db")
    if not os.path.exists(path):
        return {"sym": sym, "count": 0, "ticks": []}
    conn = _open_read(path)
    try:
        rows = conn.execute("""SELECT ts, ltp, bid, ask, vol FROM ticks
                               WHERE sym = ? ORDER BY ts DESC LIMIT ?""", (sym, n)).fetchall()
    finally:
        conn.close()
    return {"sym": sym, "count": len(rows), "ticks": [
        {"ts": r[0], "ltp": r[1], "bid": r[2], "ask": r[3], "vol": r[4]}
        for r in reversed(rows)
    ]}


@app.get("/ohlc")
async def ohlc(
    sym: str,
    date: str = Query(default=None, regex=r"^\d{8}$"),
    interval: str = Query(default="1m", regex=r"^\d+[sm]$"),
):
    """OHLC candles from stored ticks. interval: e.g. 30s, 1m, 5m, 15m."""
    date = date or time.strftime("%Y%m%d")
    path = os.path.join(DATA_DIR, f"ticks_{date}.db")
    if not os.path.exists(path):
        return {"date": date, "sym": sym, "interval": interval, "candles": []}
    unit, n = interval[-1], int(interval[:-1])
    bucket_ms = n * (1000 if unit == "s" else 60000)
    conn = _open_read(path)
    try:
        rows = conn.execute("SELECT ts, ltp, vol FROM ticks WHERE sym = ? ORDER BY ts",
                            (sym,)).fetchall()
    finally:
        conn.close()
    candles = {}
    for ts, ltp, vol in rows:
        if ltp is None: continue
        b = (ts // bucket_ms) * bucket_ms
        c = candles.get(b)
        if c is None:
            candles[b] = {"o": ltp, "h": ltp, "l": ltp, "c": ltp,
                          "v_first": vol or 0, "v_last": vol or 0}
        else:
            if ltp > c["h"]: c["h"] = ltp
            if ltp < c["l"]: c["l"] = ltp
            c["c"] = ltp
            if vol is not None: c["v_last"] = vol
    return {"date": date, "sym": sym, "interval": interval,
            "candles": [{"ts": k, "o": v["o"], "h": v["h"], "l": v["l"], "c": v["c"],
                         "v": max(0, v["v_last"] - v["v_first"])}
                        for k, v in sorted(candles.items())]}


# ---------- launcher ----------
def _sig_handler(*_):
    print("[main] shutdown signal", flush=True)
    _shutdown.set()
    q_ticks.put(None)


def main():
    signal.signal(signal.SIGTERM, _sig_handler)
    signal.signal(signal.SIGINT, _sig_handler)

    if BROKER not in BROKERS:
        print(f"[main] FATAL: unknown BROKER={BROKER!r}. Valid: {list(BROKERS)}", flush=True)
        sys.exit(1)

    threading.Thread(target=writer_loop,   daemon=True, name="writer").start()
    threading.Thread(target=cleanup_loop,  daemon=True, name="cleanup").start()
    threading.Thread(target=BROKERS[BROKER], daemon=True, name="ingest").start()
    time.sleep(2)
    print(f"[main] API on http://{API_HOST}:{API_PORT}  BROKER={BROKER}  data={DATA_DIR}",
          flush=True)
    uvicorn.run(app, host=API_HOST, port=API_PORT, log_level="warning")


if __name__ == "__main__":
    main()
