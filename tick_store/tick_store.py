"""
tick_store.py -- broker-agnostic NSE tick capture + REST query API.

Designed for a cheap 1-vCPU / 2 GB VPS. Run one instance per broker on
separate servers. Same script; switch the BROKER env var.

Storage: one SQLite file per day at $DATA_DIR/ticks_YYYYMMDD.db (WAL mode).
Query:   FastAPI on $API_PORT with endpoints /health /symbols /ticks /latest /ohlc.

NOTE ON NAMING: every variable and function in this file uses a full,
descriptive name (no single-letter or cryptic abbreviations) so the
code is self-explanatory without needing to cross-reference comments
or external documentation.
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
if hasattr(time, "tzset"):
    time.tzset()

from fastapi import FastAPI, Query, HTTPException
import uvicorn
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIGURATION CONSTANTS
# ============================================================
BROKER_NAME              = os.getenv("BROKER", "angel").lower()
DATA_DIRECTORY           = os.getenv("DATA_DIR", "./data")
TRADING_UNIVERSE         = [symbol.strip() for symbol in os.getenv("UNIVERSE", "RELIANCE,TCS").split(",")
                            if symbol.strip()]
API_HOST                 = os.getenv("API_HOST", "0.0.0.0")
API_PORT                 = int(os.getenv("API_PORT", "8080"))
FLUSH_INTERVAL_SECONDS   = float(os.getenv("FLUSH_INTERVAL_S", "5"))
FLUSH_BATCH_MAX_SIZE     = int(os.getenv("FLUSH_BATCH_MAX", "1000"))
KEEP_DAYS                = int(os.getenv("KEEP_DAYS", "90"))

os.makedirs(DATA_DIRECTORY, exist_ok=True)

# ============================================================
# SQLITE HELPERS
# ============================================================
TICKS_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
    symbol TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,           -- milliseconds since epoch
    last_traded_price REAL,
    best_bid_price REAL,
    best_ask_price REAL,
    cumulative_day_volume INTEGER,
    daily_vwap REAL,
    total_buy_quantity INTEGER,
    total_sell_quantity INTEGER,
    open_price REAL,
    previous_close_price REAL
);
CREATE INDEX IF NOT EXISTS idx_symbol_timestamp ON ticks(symbol, timestamp_ms);
"""


def database_path_for_timestamp(timestamp):
    """Returns the SQLite file path for the day that timestamp falls on."""
    date_string = time.strftime("%Y%m%d", time.localtime(timestamp))
    return os.path.join(DATA_DIRECTORY, f"ticks_{date_string}.db")


def open_database_for_writing(path):
    """Opens (creating if necessary) a SQLite database configured for
    fast, WAL-mode, autocommit writes."""
    connection = sqlite3.connect(path, isolation_level=None)   # autocommit
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA temp_store=MEMORY")
    connection.executescript(TICKS_TABLE_SCHEMA)
    return connection


def open_database_for_reading(path):
    """Opens a SQLite database in read-only mode. Safe to use
    concurrently with the writer thread's WAL-mode connection."""
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


# ============================================================
# WRITER THREAD (batched inserts)
# ============================================================
tick_queue: Queue = Queue()
writer_stats = {"received": 0, "written": 0, "flushes": 0, "errors": 0, "last_flush_timestamp": 0}
shutdown_requested = threading.Event()


def writer_loop():
    """Background thread: drains tick_queue, batches ticks in memory,
    and flushes them to the appropriate day's SQLite file either every
    FLUSH_INTERVAL_SECONDS or every FLUSH_BATCH_MAX_SIZE ticks,
    whichever comes first."""
    database_connection = None
    current_database_date = None
    pending_ticks_buffer = []
    last_flush_time = time.time()
    print("[writer] up", flush=True)

    def flush_buffer_to_database():
        nonlocal database_connection, current_database_date, last_flush_time
        if not pending_ticks_buffer:
            return

        first_tick_timestamp = pending_ticks_buffer[0]["timestamp"]
        date_string = time.strftime("%Y%m%d", time.localtime(first_tick_timestamp))
        if date_string != current_database_date:
            if database_connection:
                try:
                    database_connection.close()
                except Exception:
                    pass
            database_connection = open_database_for_writing(
                database_path_for_timestamp(first_tick_timestamp))
            current_database_date = date_string
            print(f"[writer] active db: "
                  f"{database_path_for_timestamp(first_tick_timestamp)}", flush=True)

        try:
            rows_to_insert = [
                (tick["symbol"], int(tick["timestamp"] * 1000),
                 tick.get("last_traded_price"), tick.get("best_bid_price"),
                 tick.get("best_ask_price"), tick.get("cumulative_day_volume"),
                 tick.get("daily_vwap"), tick.get("total_buy_quantity"),
                 tick.get("total_sell_quantity"), tick.get("open_price"),
                 tick.get("previous_close_price"))
                for tick in pending_ticks_buffer
            ]
            database_connection.executemany(
                """INSERT INTO ticks
                   (symbol, timestamp_ms, last_traded_price, best_bid_price,
                    best_ask_price, cumulative_day_volume, daily_vwap,
                    total_buy_quantity, total_sell_quantity, open_price,
                    previous_close_price)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                rows_to_insert)
            writer_stats["written"] += len(rows_to_insert)
            writer_stats["flushes"] += 1
            writer_stats["last_flush_timestamp"] = time.time()
        except Exception as flush_error:
            writer_stats["errors"] += 1
            print(f"[writer] flush err: {flush_error!r}", flush=True)

        pending_ticks_buffer.clear()
        last_flush_time = time.time()

    while not (shutdown_requested.is_set() and tick_queue.empty()):
        try:
            queued_tick = tick_queue.get(timeout=1.0)
            if queued_tick is None:
                break
            pending_ticks_buffer.append(queued_tick)
            writer_stats["received"] += 1
        except Empty:
            pass

        current_time = time.time()
        if pending_ticks_buffer and (
                len(pending_ticks_buffer) >= FLUSH_BATCH_MAX_SIZE
                or current_time - last_flush_time >= FLUSH_INTERVAL_SECONDS):
            flush_buffer_to_database()

    flush_buffer_to_database()   # final flush on shutdown
    if database_connection:
        try:
            database_connection.close()
        except Exception:
            pass
    print("[writer] stopped", flush=True)


# ============================================================
# CLEANUP THREAD (auto-delete old database files)
# ============================================================
def cleanup_loop():
    """Background thread: once per hour, deletes any daily database
    files (plus their WAL/SHM sidecar files) older than KEEP_DAYS."""
    while not shutdown_requested.is_set():
        try:
            cutoff_date_string = (datetime.now() - timedelta(days=KEEP_DAYS)).strftime("%Y%m%d")
            for filename in os.listdir(DATA_DIRECTORY):
                if filename.startswith("ticks_") and filename.endswith(".db"):
                    file_date_string = filename.replace("ticks_", "").replace(".db", "")
                    if file_date_string < cutoff_date_string:
                        os.remove(os.path.join(DATA_DIRECTORY, filename))
                        for sidecar_suffix in ("-wal", "-shm"):
                            sidecar_path = os.path.join(DATA_DIRECTORY, filename + sidecar_suffix)
                            if os.path.exists(sidecar_path):
                                os.remove(sidecar_path)
                        print(f"[cleanup] removed {filename} (older than {KEEP_DAYS}d)", flush=True)
        except Exception as cleanup_error:
            print(f"[cleanup] err: {cleanup_error!r}", flush=True)

        for _ in range(3600):
            if shutdown_requested.is_set():
                return
            time.sleep(1)


# ============================================================
# BROKER A: ANGEL ONE
# ============================================================
def angel_ingester():
    """Logs in to Angel SmartAPI, resolves the trading universe to
    exchange tokens, opens the market-data WebSocket in snap-quote
    mode 3, and pushes every incoming tick onto tick_queue for the
    writer thread to persist."""
    from SmartApi import SmartConnect
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
    import pyotp
    import requests

    angel_api_key      = os.getenv("ANGEL_API_KEY")
    angel_client_code  = os.getenv("ANGEL_CLIENT_CODE")
    angel_password     = os.getenv("ANGEL_PASSWORD")
    angel_totp_secret  = os.getenv("ANGEL_TOTP_SECRET")

    required_env_vars = {
        "ANGEL_API_KEY": angel_api_key,
        "ANGEL_CLIENT_CODE": angel_client_code,
        "ANGEL_PASSWORD": angel_password,
        "ANGEL_TOTP_SECRET": angel_totp_secret,
    }
    for env_var_name, env_var_value in required_env_vars.items():
        if not env_var_value:
            print(f"[angel] FATAL missing {env_var_name}")
            return

    angel_connection = SmartConnect(api_key=angel_api_key)
    login_session = angel_connection.generateSession(
        angel_client_code, angel_password, pyotp.TOTP(angel_totp_secret).now())
    if not login_session or not login_session.get("data"):
        print(f"[angel] FATAL login: {login_session}")
        return

    auth_token = login_session["data"]["jwtToken"]
    feed_token = angel_connection.getfeedToken()

    scrip_master_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    scrip_master_list = requests.get(scrip_master_url, timeout=60).json()
    nse_symbol_to_token = {
        entry["symbol"].replace("-EQ", ""): entry["token"]
        for entry in scrip_master_list
        if entry.get("exch_seg") == "NSE" and entry.get("symbol", "").endswith("-EQ")
    }
    token_to_symbol_map = {
        nse_symbol_to_token[symbol]: symbol
        for symbol in TRADING_UNIVERSE if symbol in nse_symbol_to_token
    }
    token_list = list(token_to_symbol_map.keys())
    bid_ask_is_swapped = os.getenv("ANGEL_BIDASK_SWAPPED", "0") == "1"
    print(f"[angel] login OK, subscribing {len(token_list)}/{len(TRADING_UNIVERSE)} symbols",
          flush=True)

    try:
        websocket_client = SmartWebSocketV2(
            auth_token, angel_api_key, angel_client_code, feed_token,
            max_retry_attempt=10, retry_strategy=1,
            retry_delay=5, retry_multiplier=2, retry_duration=60)
    except TypeError:
        websocket_client = SmartWebSocketV2(
            auth_token, angel_api_key, angel_client_code, feed_token)

    def on_tick_received(_websocket, market_data_message):
        symbol = token_to_symbol_map.get(str(market_data_message.get("token")))
        if not symbol:
            return

        best_5_buy_levels  = market_data_message.get("best_5_buy_data") or []
        best_5_sell_levels = market_data_message.get("best_5_sell_data") or []
        if bid_ask_is_swapped:
            best_5_buy_levels, best_5_sell_levels = best_5_sell_levels, best_5_buy_levels

        tick_queue.put({
            "symbol": symbol,
            "timestamp": time.time(),
            "last_traded_price": market_data_message.get("last_traded_price", 0) / 100.0,
            "best_bid_price": (best_5_buy_levels[0]["price"] / 100.0) if best_5_buy_levels else 0.0,
            "best_ask_price": (best_5_sell_levels[0]["price"] / 100.0) if best_5_sell_levels else 0.0,
            "cumulative_day_volume": market_data_message.get("volume_trade_for_the_day", 0),
            "daily_vwap": market_data_message.get("average_traded_price", 0) / 100.0,
            "total_buy_quantity": market_data_message.get("total_buy_quantity", 0),
            "total_sell_quantity": market_data_message.get("total_sell_quantity", 0),
            "open_price": market_data_message.get("open_price_of_the_day", 0) / 100.0,
            "previous_close_price": market_data_message.get("closed_price", 0) / 100.0,
        })

    websocket_client.on_data = on_tick_received
    websocket_client.on_open = lambda _websocket: websocket_client.subscribe(
        "tickstore", 3, [{"exchangeType": 1, "tokens": token_list}])
    websocket_client.on_error = lambda _websocket, error: print(
        f"[angel] WS error: {error}", flush=True)
    websocket_client.on_close = lambda _websocket: print("[angel] WS closed", flush=True)
    websocket_client.connect()


# ============================================================
# BROKER B: UPSTOX (template -- fill in per Upstox v2 SDK docs)
# ============================================================
def upstox_ingester():
    """Upstox WebSocket ingester -- TEMPLATE.
    Install:  pip install upstox-python-sdk
    Docs:     https://upstox.com/developer/api-documentation/websocket
    Auth:     access token from OAuth flow, refreshed daily

    To implement, follow this shape (uncomment and adapt):

        from upstox_client import Configuration, ApiClient
        from upstox_client.feeder import MarketDataStreamerV3

        upstox_configuration = Configuration()
        upstox_configuration.access_token = os.getenv("UPSTOX_ACCESS_TOKEN")
        upstox_api_client = ApiClient(upstox_configuration)
        instrument_keys = [
            os.getenv(f"UPSTOX_INSTRUMENT_{symbol}", "")
            for symbol in TRADING_UNIVERSE
        ]
        instrument_keys = [key for key in instrument_keys if key]
        market_data_streamer = MarketDataStreamerV3(
            upstox_api_client, instrument_keys, "full")

        def on_market_data_message(message):
            # Field names below depend on the installed SDK version;
            # extract and push onto tick_queue in this shape:
            tick_queue.put({
                "symbol": "<symbol>", "timestamp": time.time(),
                "last_traded_price": "<ltp>", "best_bid_price": "<bid>",
                "best_ask_price": "<ask>",
                "cumulative_day_volume": "<volume>",
                "daily_vwap": "<vwap>",
                "total_buy_quantity": "<buy_qty>",
                "total_sell_quantity": "<sell_qty>",
                "open_price": "<open>", "previous_close_price": "<prev_close>",
            })

        market_data_streamer.on("message", on_market_data_message)
        market_data_streamer.connect()
    """
    print("[upstox] TODO: fill in upstox_ingester() -- see the docstring in code", flush=True)
    while not shutdown_requested.is_set():
        time.sleep(60)


INGESTER_FUNCTIONS_BY_BROKER_NAME = {"angel": angel_ingester, "upstox": upstox_ingester}


# ============================================================
# FASTAPI QUERY API
# ============================================================
fastapi_app = FastAPI(title=f"Tick Store ({BROKER_NAME})",
                       description="NSE tick capture + query API")


@fastapi_app.get("/health")
async def get_health_status():
    return {
        "ok": True,
        "broker": BROKER_NAME,
        "universe": TRADING_UNIVERSE,
        "queue_size": tick_queue.qsize(),
        "stats": writer_stats,
        "data_directory": DATA_DIRECTORY,
        "keep_days": KEEP_DAYS,
    }


@fastapi_app.get("/dates")
async def list_available_dates():
    database_filenames = sorted([
        filename for filename in os.listdir(DATA_DIRECTORY)
        if filename.startswith("ticks_") and filename.endswith(".db")
    ])
    return {"dates": [filename.replace("ticks_", "").replace(".db", "")
                      for filename in database_filenames]}


@fastapi_app.get("/symbols")
async def list_symbols_for_date(date: str = Query(default=None, regex=r"^\d{8}$")):
    date = date or time.strftime("%Y%m%d")
    database_path = os.path.join(DATA_DIRECTORY, f"ticks_{date}.db")
    if not os.path.exists(database_path):
        return {"date": date, "symbols": []}

    connection = open_database_for_reading(database_path)
    try:
        rows = connection.execute("SELECT DISTINCT symbol FROM ticks ORDER BY symbol").fetchall()
    finally:
        connection.close()
    return {"date": date, "symbols": [row[0] for row in rows]}


def parse_time_of_day_argument(date_string, time_string, argument_name):
    """Parses an 'HH:MM' or 'HH:MM:SS' IST time string (combined with a
    'YYYYMMDD' date string) into a millisecond epoch timestamp."""
    time_parts = time_string.split(":")
    hour, minute = int(time_parts[0]), int(time_parts[1])
    second = int(time_parts[2]) if len(time_parts) > 2 else 0
    try:
        parsed_datetime = datetime.strptime(
            f"{date_string} {hour:02d}:{minute:02d}:{second:02d}", "%Y%m%d %H:%M:%S")
        return int(parsed_datetime.timestamp() * 1000)
    except Exception as parse_error:
        raise HTTPException(400, f"bad {argument_name}: {parse_error}")


@fastapi_app.get("/ticks")
async def get_raw_ticks(
    symbol: str,
    date: str = Query(default=None, regex=r"^\d{8}$"),
    start: str = Query(default=None, description="HH:MM or HH:MM:SS IST"),
    end: str = Query(default=None, description="HH:MM or HH:MM:SS IST"),
    limit: int = Query(default=10000, ge=1, le=100000),
):
    date = date or time.strftime("%Y%m%d")
    database_path = os.path.join(DATA_DIRECTORY, f"ticks_{date}.db")
    if not os.path.exists(database_path):
        return {"date": date, "symbol": symbol, "count": 0, "ticks": []}

    sql_query = ("SELECT timestamp_ms, last_traded_price, best_bid_price, best_ask_price, "
                 "cumulative_day_volume, daily_vwap, total_buy_quantity, total_sell_quantity "
                 "FROM ticks WHERE symbol = ?")
    query_arguments = [symbol]
    if start:
        sql_query += " AND timestamp_ms >= ?"
        query_arguments.append(parse_time_of_day_argument(date, start, "start"))
    if end:
        sql_query += " AND timestamp_ms <= ?"
        query_arguments.append(parse_time_of_day_argument(date, end, "end"))
    sql_query += " ORDER BY timestamp_ms LIMIT ?"
    query_arguments.append(limit)

    connection = open_database_for_reading(database_path)
    try:
        rows = connection.execute(sql_query, query_arguments).fetchall()
    finally:
        connection.close()

    return {"date": date, "symbol": symbol, "count": len(rows), "ticks": [
        {"timestamp_ms": row[0], "last_traded_price": row[1], "best_bid_price": row[2],
         "best_ask_price": row[3], "cumulative_day_volume": row[4], "daily_vwap": row[5],
         "total_buy_quantity": row[6], "total_sell_quantity": row[7]}
        for row in rows
    ]}


@fastapi_app.get("/latest")
async def get_latest_ticks(symbol: str, count: int = Query(default=100, ge=1, le=10000)):
    date = time.strftime("%Y%m%d")
    database_path = os.path.join(DATA_DIRECTORY, f"ticks_{date}.db")
    if not os.path.exists(database_path):
        return {"symbol": symbol, "count": 0, "ticks": []}

    connection = open_database_for_reading(database_path)
    try:
        rows = connection.execute(
            """SELECT timestamp_ms, last_traded_price, best_bid_price, best_ask_price,
                      cumulative_day_volume
               FROM ticks WHERE symbol = ? ORDER BY timestamp_ms DESC LIMIT ?""",
            (symbol, count)).fetchall()
    finally:
        connection.close()

    return {"symbol": symbol, "count": len(rows), "ticks": [
        {"timestamp_ms": row[0], "last_traded_price": row[1], "best_bid_price": row[2],
         "best_ask_price": row[3], "cumulative_day_volume": row[4]}
        for row in reversed(rows)
    ]}


@fastapi_app.get("/ohlc")
async def get_ohlc_candles(
    symbol: str,
    date: str = Query(default=None, regex=r"^\d{8}$"),
    interval: str = Query(default="1m", regex=r"^\d+[sm]$"),
):
    """Builds OHLC candles from stored ticks. interval examples: 30s, 1m, 5m, 15m."""
    date = date or time.strftime("%Y%m%d")
    database_path = os.path.join(DATA_DIRECTORY, f"ticks_{date}.db")
    if not os.path.exists(database_path):
        return {"date": date, "symbol": symbol, "interval": interval, "candles": []}

    interval_unit, interval_count = interval[-1], int(interval[:-1])
    bucket_size_ms = interval_count * (1000 if interval_unit == "s" else 60000)

    connection = open_database_for_reading(database_path)
    try:
        rows = connection.execute(
            "SELECT timestamp_ms, last_traded_price, cumulative_day_volume "
            "FROM ticks WHERE symbol = ? ORDER BY timestamp_ms",
            (symbol,)).fetchall()
    finally:
        connection.close()

    candles_by_bucket_start = {}
    for timestamp_ms, last_traded_price, cumulative_day_volume in rows:
        if last_traded_price is None:
            continue
        bucket_start_ms = (timestamp_ms // bucket_size_ms) * bucket_size_ms
        candle = candles_by_bucket_start.get(bucket_start_ms)
        if candle is None:
            candles_by_bucket_start[bucket_start_ms] = {
                "open": last_traded_price, "high": last_traded_price,
                "low": last_traded_price, "close": last_traded_price,
                "volume_at_bucket_start": cumulative_day_volume or 0,
                "volume_at_bucket_end": cumulative_day_volume or 0,
            }
        else:
            if last_traded_price > candle["high"]:
                candle["high"] = last_traded_price
            if last_traded_price < candle["low"]:
                candle["low"] = last_traded_price
            candle["close"] = last_traded_price
            if cumulative_day_volume is not None:
                candle["volume_at_bucket_end"] = cumulative_day_volume

    return {"date": date, "symbol": symbol, "interval": interval, "candles": [
        {"timestamp_ms": bucket_start_ms,
         "open": candle["open"], "high": candle["high"],
         "low": candle["low"], "close": candle["close"],
         "volume": max(0, candle["volume_at_bucket_end"] - candle["volume_at_bucket_start"])}
        for bucket_start_ms, candle in sorted(candles_by_bucket_start.items())
    ]}


# ============================================================
# LAUNCHER
# ============================================================
def handle_shutdown_signal(*_args):
    print("[main] shutdown signal", flush=True)
    shutdown_requested.set()
    tick_queue.put(None)


def main():
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    signal.signal(signal.SIGINT, handle_shutdown_signal)

    if BROKER_NAME not in INGESTER_FUNCTIONS_BY_BROKER_NAME:
        print(f"[main] FATAL: unknown BROKER={BROKER_NAME!r}. "
              f"Valid: {list(INGESTER_FUNCTIONS_BY_BROKER_NAME)}", flush=True)
        sys.exit(1)

    threading.Thread(target=writer_loop, daemon=True, name="writer").start()
    threading.Thread(target=cleanup_loop, daemon=True, name="cleanup").start()
    threading.Thread(target=INGESTER_FUNCTIONS_BY_BROKER_NAME[BROKER_NAME],
                      daemon=True, name="ingest").start()

    time.sleep(2)
    print(f"[main] API on http://{API_HOST}:{API_PORT}  "
          f"BROKER={BROKER_NAME}  data={DATA_DIRECTORY}", flush=True)
    uvicorn.run(fastapi_app, host=API_HOST, port=API_PORT, log_level="warning")


if __name__ == "__main__":
    main()
