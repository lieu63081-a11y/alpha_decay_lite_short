import os
import sys
import time
import signal
import threading

os.environ["TZ"] = "Asia/Kolkata"
if hasattr(time, "tzset"):
    time.tzset()
elif os.name != "posix":
    print("[STARTUP] FATAL: this script requires time.tzset() (Unix-only) "
          "for correct IST session-window calculations. Detected a "
          "non-Unix platform (Windows?) where TZ env var alone does not "
          "affect time.localtime(). Run this on a Linux/Unix host instead.",
          flush=True)
    sys.exit(1)

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
import redis.asyncio as redis_asyncio
from dotenv import load_dotenv

load_dotenv()

ZMQ_TICKS_ADDRESS   = "ipc:///tmp/alpha_ticks.ipc"
ZMQ_SCORES_ADDRESS  = "ipc:///tmp/alpha_scores.ipc"
REDIS_URL            = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MUTE_FILE_PATH       = os.getenv("MUTE_FILE", "/tmp/alpha_mute.flag")
TRADING_UNIVERSE     = [symbol.strip() for symbol in
                         os.getenv("UNIVERSE", "RELIANCE,TCS,HDFCBANK").split(",")
                         if symbol.strip()]
SCRIP_MASTER_URL     = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

TELEGRAM_BOT_TOKEN   = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "").strip()

ANGEL_BID_ASK_IS_SWAPPED = os.getenv("ANGEL_BIDASK_SWAPPED", "0").strip() == "1"

SPREAD_RATIO_MAX            = 0.0015
EMA_TIME_CONSTANT_SECONDS   = 1.0

COOLDOWN_SECONDS_BY_MODE = {"MOMENTUM": 300, "MEAN_REVERSION": 45, "GAP_FADE": 180}

ZMQ_HIGH_WATER_MARK      = 10000
LATENCY_WARNING_MS       = 100
MUTE_STATUS_CACHE_SECONDS = 1.0
RVOL_WARMUP_SECONDS      = 300
RESET_DROP_FRACTION_THRESHOLD = 0.5
PEAK_SCORE_HALF_LIFE_SECONDS = 60.0
AUTO_RESTART_AFTER_HOURS = float(os.getenv("AUTO_RESTART_HOURS", "20"))
TICK_STARVATION_WATCHDOG_SECONDS = 90

EXIT_CODE_PLANNED_RESTART = 2
EXIT_CODE_FATAL_CONFIG    = 3

PROCESS_HEARTBEAT_TTL_SECONDS      = 3
DATA_FLOW_HEARTBEAT_TTL_SECONDS    = 5
HEARTBEAT_WRITE_INTERVAL_SECONDS   = 1

EXIT_ALERT_SCORE_THRESHOLD   = 2.0
EXIT_ALERT_COOLDOWN_SECONDS  = 300
TRACKED_POSITION_TTL_SECONDS = 6 * 3600
PENDING_ALERT_TTL_SECONDS    = 24 * 3600

def clip_value(value, minimum, maximum):
    return max(minimum, min(maximum, value))

def is_within_trading_hours(timestamp):
    local_time = time.localtime(timestamp)
    if local_time.tm_wday >= 5:
        return False
    hour, minute = local_time.tm_hour, local_time.tm_min
    after_open = (hour > 9) or (hour == 9 and minute >= 15)
    before_close = (hour < 15) or (hour == 15 and minute <= 30)
    return after_open and before_close

class RollingWindowSum:
    __slots__ = ("window_seconds", "entries", "running_sum")

    def __init__(self, window_seconds):
        self.window_seconds = window_seconds
        self.entries = deque()
        self.running_sum = 0.0

    def add(self, timestamp, value):
        if self.entries and timestamp < self.entries[-1][0]:
            return
        self.entries.append((timestamp, value))
        self.running_sum += value
        eviction_cutoff = timestamp - self.window_seconds
        while self.entries and self.entries[0][0] < eviction_cutoff:
            self.running_sum -= self.entries.popleft()[1]

    def span_seconds(self):
        if len(self.entries) < 2:
            return 0.0
        return self.entries[-1][0] - self.entries[0][0]

def start_heartbeat_thread(heartbeat_name, redis_client, additional_check=None):

    def heartbeat_loop():
        while True:
            try:
                redis_client.setex(f"alpha:hb:{heartbeat_name}",
                                    PROCESS_HEARTBEAT_TTL_SECONDS, "1")
                if additional_check:
                    extra_name, is_alive = additional_check()
                    if is_alive:
                        redis_client.setex(f"alpha:hb:{extra_name}",
                                            DATA_FLOW_HEARTBEAT_TTL_SECONDS, "1")
            except Exception:
                pass
            time.sleep(HEARTBEAT_WRITE_INTERVAL_SECONDS)

    threading.Thread(target=heartbeat_loop, daemon=True,
                      name=f"heartbeat-{heartbeat_name}").start()

def download_scrip_master_with_retry():
    max_attempts = 4
    retry_delay_seconds = 3
    last_error = None
    for attempt_number in range(1, max_attempts + 1):
        try:
            return requests.get(SCRIP_MASTER_URL, timeout=60).json()
        except (requests.exceptions.RequestException, ValueError) as download_error:
            last_error = download_error
            print(f"[A] scrip master download failed (attempt {attempt_number}/"
                  f"{max_attempts}): {download_error!r}", flush=True)
            if attempt_number < max_attempts:
                time.sleep(retry_delay_seconds)
                retry_delay_seconds *= 2
    raise last_error

def resolve_nse_equity_tokens(symbol_list):
    start_time = time.time()
    print("[A] Downloading scrip master (~4 MB)...", flush=True)
    scrip_master_list = download_scrip_master_with_retry()
    nse_symbol_to_token = {
        entry["symbol"].replace("-EQ", ""): entry["token"]
        for entry in scrip_master_list
        if entry.get("exch_seg") == "NSE" and entry.get("symbol", "").endswith("-EQ")
    }
    resolved_token_to_symbol = {
        nse_symbol_to_token[symbol]: symbol
        for symbol in symbol_list if symbol in nse_symbol_to_token
    }
    missing_symbols = [symbol for symbol in symbol_list if symbol not in nse_symbol_to_token]
    elapsed_seconds = time.time() - start_time
    print(f"[A] Scrip master loaded in {elapsed_seconds:.1f}s, "
          f"resolved {len(resolved_token_to_symbol)}/{len(symbol_list)} symbols", flush=True)
    if missing_symbols:
        print(f"[A] Symbols NOT found: {missing_symbols}", flush=True)
    return resolved_token_to_symbol

def data_factory():
    from SmartApi import SmartConnect
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
    import pyotp

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
    missing_env_vars = [name for name, value in required_env_vars.items() if not value]
    if missing_env_vars:
        print(f"[A] FATAL missing env vars: {missing_env_vars}. Fill .env and restart.")
        os._exit(EXIT_CODE_FATAL_CONFIG)

    print("[A] Logging in to Angel...", flush=True)
    angel_connection = SmartConnect(api_key=angel_api_key)
    try:
        current_totp_code = pyotp.TOTP(angel_totp_secret).now()
    except Exception as totp_error:
        print(f"[A] FATAL invalid TOTP secret: {totp_error}. ANGEL_TOTP_SECRET must be base32.")
        os._exit(EXIT_CODE_FATAL_CONFIG)

    login_session = angel_connection.generateSession(
        angel_client_code, angel_password, current_totp_code)
    if not login_session or not login_session.get("data"):
        error_message = (login_session or {}).get("message", "unknown")
        print(f"[A] FATAL Angel login failed: {error_message}")
        print("     Common fixes:")
        print("       - ANGEL_PASSWORD must be your 4-digit MPIN (not web login password)")
        print("       - ANGEL_TOTP_SECRET must be the full base32 secret (not 6-digit code)")
        print("       - Ensure system time is NTP-synced: `timedatectl`")
        os._exit(EXIT_CODE_FATAL_CONFIG)

    auth_token = login_session["data"]["jwtToken"]
    feed_token = angel_connection.getfeedToken()
    print(f"[A] Login OK, client={angel_client_code}", flush=True)

    if AUTO_RESTART_AFTER_HOURS > 0:
        def exit_for_scheduled_restart():
            print(f"[A] JWT nearing expiry ({AUTO_RESTART_AFTER_HOURS}h up), "
                  f"exiting for restart", flush=True)
            os._exit(EXIT_CODE_PLANNED_RESTART)

        restart_timer = threading.Timer(AUTO_RESTART_AFTER_HOURS * 3600,
                                         exit_for_scheduled_restart)
        restart_timer.daemon = True
        restart_timer.start()

    token_to_symbol_map = resolve_nse_equity_tokens(TRADING_UNIVERSE)
    token_list = list(token_to_symbol_map.keys())

    zmq_context = zmq.Context()
    tick_publisher = zmq_context.socket(zmq.PUB)
    tick_publisher.setsockopt(zmq.SNDHWM, ZMQ_HIGH_WATER_MARK)
    tick_publisher.bind(ZMQ_TICKS_ADDRESS)
    time.sleep(1.5)
    print(f"[A] Data Factory up, {len(token_list)} tokens mode=3", flush=True)

    heartbeat_redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    last_tick_received_at = {"timestamp": 0.0}
    last_heartbeat_write_from_tick = {"timestamp": 0.0}

    def check_data_is_flowing():
        seconds_since_last_tick = time.time() - last_tick_received_at["timestamp"]
        return "data", seconds_since_last_tick < DATA_FLOW_HEARTBEAT_TTL_SECONDS

    start_heartbeat_thread("A", heartbeat_redis_client, additional_check=check_data_is_flowing)

    def watch_for_tick_starvation():
        while True:
            time.sleep(15)
            now = time.time()
            if not is_within_trading_hours(now):
                continue
            seconds_since_last_tick = now - last_tick_received_at["timestamp"]
            if (last_tick_received_at["timestamp"] > 0
                    and seconds_since_last_tick > TICK_STARVATION_WATCHDOG_SECONDS):
                print(f"[A] WATCHDOG: no tick in {seconds_since_last_tick:.0f}s during "
                      f"market hours (possible zombie connection) -- forcing restart",
                      flush=True)
                os._exit(EXIT_CODE_PLANNED_RESTART)

    watchdog_thread = threading.Thread(target=watch_for_tick_starvation, daemon=True)
    watchdog_thread.start()

    try:
        websocket_client = SmartWebSocketV2(
            auth_token, angel_api_key, angel_client_code, feed_token,
            max_retry_attempt=10, retry_strategy=1,
            retry_delay=5, retry_multiplier=2, retry_duration=60)
    except TypeError:
        print("[A] SDK does not accept retry kwargs, using defaults", flush=True)
        websocket_client = SmartWebSocketV2(
            auth_token, angel_api_key, angel_client_code, feed_token)

    def on_tick_received(_websocket, market_data_message):
        symbol = token_to_symbol_map.get(str(market_data_message.get("token")))
        if not symbol:
            return
        now = time.time()
        last_tick_received_at["timestamp"] = now

        if now - last_heartbeat_write_from_tick["timestamp"] >= HEARTBEAT_WRITE_INTERVAL_SECONDS:
            try:
                heartbeat_redis_client.setex(
                    "alpha:hb:data", DATA_FLOW_HEARTBEAT_TTL_SECONDS, "1")
                last_heartbeat_write_from_tick["timestamp"] = now
            except Exception:
                pass

        best_5_buy_levels  = market_data_message.get("best_5_buy_data") or []
        best_5_sell_levels = market_data_message.get("best_5_sell_data") or []
        if ANGEL_BID_ASK_IS_SWAPPED:
            best_5_buy_levels, best_5_sell_levels = best_5_sell_levels, best_5_buy_levels

        tick_data = {
            "symbol": symbol,
            "timestamp": time.time(),
            "last_traded_price": (market_data_message.get("last_traded_price") or 0) / 100.0,
            "best_bid_price": ((best_5_buy_levels[0].get("price") or 0) / 100.0) if best_5_buy_levels else 0.0,
            "best_ask_price": ((best_5_sell_levels[0].get("price") or 0) / 100.0) if best_5_sell_levels else 0.0,
            "cumulative_day_volume": market_data_message.get("volume_trade_for_the_day") or 0,
            "daily_vwap": (market_data_message.get("average_traded_price") or 0) / 100.0,
            "total_buy_quantity": market_data_message.get("total_buy_quantity") or 0,
            "total_sell_quantity": market_data_message.get("total_sell_quantity") or 0,
            "top5_buy_quantity":  sum((lvl.get("quantity") or 0) for lvl in best_5_buy_levels[:5]),
            "top5_sell_quantity": sum((lvl.get("quantity") or 0) for lvl in best_5_sell_levels[:5]),
            "open_price": (market_data_message.get("open_price_of_the_day") or 0) / 100.0,
            "previous_close_price": (market_data_message.get("closed_price") or 0) / 100.0,
        }
        try:
            tick_publisher.send_multipart(
                [symbol.encode(), json.dumps(tick_data).encode()], zmq.DONTWAIT)
        except zmq.Again:
            pass

    websocket_client.on_data = on_tick_received
    websocket_client.on_open = lambda _websocket: websocket_client.subscribe(
        "adl", 3, [{"exchangeType": 1, "tokens": token_list}])
    websocket_client.on_error = lambda _websocket, error: print(
        f"[A] WS error: {error}", flush=True)
    websocket_client.on_close = lambda _websocket: print(
        "[A] WS closed (SDK will reconnect)", flush=True)

    def handle_stop_signal(_signum, _frame):
        print("[A] shutdown signal received, closing WebSocket...", flush=True)
        try:
            websocket_client.close_connection()
        except Exception as close_error:
            print(f"[A] WS close error during shutdown: {close_error!r}", flush=True)

    signal.signal(signal.SIGTERM, handle_stop_signal)
    signal.signal(signal.SIGINT, handle_stop_signal)

    websocket_client.connect()

    print("[A] shutting down (WebSocket connection closed)", flush=True)
    tick_publisher.close()
    zmq_context.term()

@dataclass
class SymbolState:
    volume_sum_last_60_seconds: RollingWindowSum = field(
        default_factory=lambda: RollingWindowSum(60))
    volume_sum_last_20_minutes: RollingWindowSum = field(
        default_factory=lambda: RollingWindowSum(1200))
    aggressive_signed_volume_30s: RollingWindowSum = field(
        default_factory=lambda: RollingWindowSum(30))
    aggressive_absolute_volume_30s: RollingWindowSum = field(
        default_factory=lambda: RollingWindowSum(30))
    last_traded_price_history_30s: deque = field(default_factory=deque)
    last_cumulative_volume: int = -1
    last_traded_price: float = 0.0
    smoothed_ema_score: float = 0.0
    last_tick_timestamp: float = 0.0
    peak_score_amplitude: float = 0.0

def calculate_vwap_distance(_symbol_state, tick_data):
    if not tick_data["daily_vwap"] or not tick_data["last_traded_price"]:
        return 0.0
    percent_distance = ((tick_data["last_traded_price"] - tick_data["daily_vwap"])
                         / tick_data["daily_vwap"] * 100)
    return clip_value(percent_distance * 5.0, -2.0, 2.0)

def calculate_relative_volume(symbol_state, _tick_data):
    window = symbol_state.volume_sum_last_20_minutes
    if window.span_seconds() < RVOL_WARMUP_SECONDS:
        return 1.0
    baseline_volume_sum = max(
        window.running_sum - symbol_state.volume_sum_last_60_seconds.running_sum, 0.0)
    elapsed_baseline_minutes = max(1.0, (window.span_seconds() - 60) / 60)
    baseline_minutes = min(19.0, elapsed_baseline_minutes)
    baseline_volume_per_minute = baseline_volume_sum / baseline_minutes
    return symbol_state.volume_sum_last_60_seconds.running_sum / max(baseline_volume_per_minute, 1.0)

def calculate_aggressive_buy_pressure(symbol_state, _tick_data):
    gross_classified_volume = symbol_state.aggressive_absolute_volume_30s.running_sum
    if gross_classified_volume < 100:
        return 0.0
    net_classified_volume = symbol_state.aggressive_signed_volume_30s.running_sum
    return clip_value(net_classified_volume / gross_classified_volume * 2.0, -2.0, 2.0)

def calculate_absorption(symbol_state, tick_data):
    price_history = symbol_state.last_traded_price_history_30s
    if len(price_history) < 5 or not tick_data["last_traded_price"]:
        return 0.0
    lowest_price_in_window = min(price for _, price in price_history)
    highest_price_in_window = max(price for _, price in price_history)
    price_range_ratio = ((highest_price_in_window - lowest_price_in_window)
                          / tick_data["last_traded_price"])
    if price_range_ratio > 0.001:
        return 0.0

    if symbol_state.aggressive_signed_volume_30s.span_seconds() < 10:
        return 0.0

    gross_classified_volume = symbol_state.aggressive_absolute_volume_30s.running_sum
    if gross_classified_volume < 100:
        return 0.0

    net_to_gross_ratio = (symbol_state.aggressive_signed_volume_30s.running_sum
                          / gross_classified_volume)
    if net_to_gross_ratio < -0.4:
        return 1.5
    if net_to_gross_ratio > 0.4:
        return -1.5
    return 0.0

def calculate_book_imbalance(_symbol_state, tick_data):
    top5_buy_quantity  = tick_data["top5_buy_quantity"]
    top5_sell_quantity = tick_data["top5_sell_quantity"]
    combined_quantity = top5_buy_quantity + top5_sell_quantity
    if not combined_quantity:
        return 0.0
    imbalance_ratio = (top5_buy_quantity - top5_sell_quantity) / combined_quantity
    return clip_value(imbalance_ratio * 2, -2.0, 2.0)

def calculate_spread_ratio(_symbol_state, tick_data):
    if (tick_data["last_traded_price"] and tick_data["best_bid_price"]
            and tick_data["best_ask_price"]):
        return ((tick_data["best_ask_price"] - tick_data["best_bid_price"])
                / tick_data["last_traded_price"])
    return 1.0

def calculate_gap_fade_unrestricted(tick_data):
    if not tick_data["previous_close_price"] or not tick_data["open_price"]:
        return 0.0
    gap_percent = ((tick_data["open_price"] - tick_data["previous_close_price"])
                   / tick_data["previous_close_price"]) * 100
    return clip_value(-gap_percent, -3.0, 3.0)

def calculate_gap_fade(_symbol_state, tick_data):
    local_time = time.localtime(tick_data["timestamp"])
    is_within_gap_window = (local_time.tm_hour == 9
                            and 15 <= local_time.tm_min < 30)
    if not is_within_gap_window:
        return 0.0
    return calculate_gap_fade_unrestricted(tick_data)

def calculate_session_weight(_symbol_state, tick_data):
    local_time = time.localtime(tick_data["timestamp"])
    hour, minute = local_time.tm_hour, local_time.tm_min
    if hour == 9 and 15 <= minute < 45:
        return 1.2
    if (hour == 14 and minute >= 45) or (hour == 15 and minute <= 30):
        return 1.1
    return 0.8

def compute_feature_score(symbol_state, tick_data):
    relative_volume = calculate_relative_volume(symbol_state, tick_data)

    spread_ratio = calculate_spread_ratio(symbol_state, tick_data)
    if spread_ratio > SPREAD_RATIO_MAX:
        return 0.0, relative_volume, 0.0, calculate_gap_fade_unrestricted(tick_data)

    vwap_distance_score      = calculate_vwap_distance(symbol_state, tick_data)
    aggressive_buy_score     = calculate_aggressive_buy_pressure(symbol_state, tick_data)
    absorption_score         = calculate_absorption(symbol_state, tick_data)
    book_imbalance_score     = calculate_book_imbalance(symbol_state, tick_data)
    gap_fade_raw_score       = calculate_gap_fade(symbol_state, tick_data)
    gap_fade_raw_score_unrestricted = calculate_gap_fade_unrestricted(tick_data)
    session_weight           = calculate_session_weight(symbol_state, tick_data)

    if aggressive_buy_score * absorption_score >= 0:
        combined_flow_score = (max(aggressive_buy_score, absorption_score)
                               + 0.4 * min(aggressive_buy_score, absorption_score))
    else:
        combined_flow_score = aggressive_buy_score + absorption_score

    composite_score = clip_value(
        session_weight * (2.0 * vwap_distance_score
                          + 1.5 * combined_flow_score
                          + 1.5 * book_imbalance_score
                          + 1.0 * gap_fade_raw_score),
        -10.0, 10.0)
    return composite_score, relative_volume, gap_fade_raw_score, gap_fade_raw_score_unrestricted

def classify_trade_direction(tick_data, previous_last_traded_price):
    if tick_data["best_bid_price"] and tick_data["best_ask_price"]:
        midpoint_price = (tick_data["best_bid_price"] + tick_data["best_ask_price"]) / 2
    else:
        midpoint_price = tick_data["last_traded_price"]

    if tick_data["last_traded_price"] > midpoint_price:
        return 1
    if tick_data["last_traded_price"] < midpoint_price:
        return -1
    if previous_last_traded_price and tick_data["last_traded_price"] > previous_last_traded_price:
        return 1
    if previous_last_traded_price and tick_data["last_traded_price"] < previous_last_traded_price:
        return -1
    return 0

def update_rolling_windows(symbol_state, tick_data):
    current_timestamp = tick_data["timestamp"]

    if symbol_state.last_cumulative_volume < 0:
        symbol_state.last_cumulative_volume = tick_data["cumulative_day_volume"]
        delta_volume = 0
    elif tick_data["cumulative_day_volume"] >= symbol_state.last_cumulative_volume:
        delta_volume = (tick_data["cumulative_day_volume"]
                        - symbol_state.last_cumulative_volume)
        symbol_state.last_cumulative_volume = tick_data["cumulative_day_volume"]
    else:
        drop_amount = symbol_state.last_cumulative_volume - tick_data["cumulative_day_volume"]
        looks_like_a_reset = (
            symbol_state.last_cumulative_volume > 0
            and drop_amount > symbol_state.last_cumulative_volume * RESET_DROP_FRACTION_THRESHOLD)
        if looks_like_a_reset:
            symbol_state.last_cumulative_volume = max(
                tick_data["cumulative_day_volume"], 0)
        delta_volume = 0

    symbol_state.volume_sum_last_60_seconds.add(current_timestamp, delta_volume)
    symbol_state.volume_sum_last_20_minutes.add(current_timestamp, delta_volume)

    trade_direction = classify_trade_direction(tick_data, symbol_state.last_traded_price)
    symbol_state.last_traded_price = tick_data["last_traded_price"]

    trade_is_classified = trade_direction != 0 and delta_volume > 0
    signed_volume_contribution = (trade_direction * delta_volume) if trade_is_classified else 0.0
    absolute_volume_contribution = delta_volume if trade_is_classified else 0.0
    symbol_state.aggressive_signed_volume_30s.add(current_timestamp, signed_volume_contribution)
    symbol_state.aggressive_absolute_volume_30s.add(current_timestamp, absolute_volume_contribution)

    price_history = symbol_state.last_traded_price_history_30s
    if not price_history or current_timestamp >= price_history[-1][0]:
        price_history.append((current_timestamp, tick_data["last_traded_price"]))
        price_history_cutoff = current_timestamp - 30
        while price_history and price_history[0][0] < price_history_cutoff:
            price_history.popleft()

def alpha_engine():
    zmq_context = zmq.Context()

    tick_subscriber = zmq_context.socket(zmq.SUB)
    tick_subscriber.setsockopt(zmq.RCVHWM, ZMQ_HIGH_WATER_MARK)
    tick_subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    tick_subscriber.connect(ZMQ_TICKS_ADDRESS)

    score_publisher = zmq_context.socket(zmq.PUB)
    score_publisher.setsockopt(zmq.SNDHWM, ZMQ_HIGH_WATER_MARK)
    score_publisher.bind(ZMQ_SCORES_ADDRESS)

    heartbeat_redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    start_heartbeat_thread("B", heartbeat_redis_client)

    stop_requested = threading.Event()

    def handle_stop_signal(_signum, _frame):
        stop_requested.set()

    signal.signal(signal.SIGTERM, handle_stop_signal)
    signal.signal(signal.SIGINT, handle_stop_signal)

    RECEIVE_POLL_TIMEOUT_MS = 1000
    poller = zmq.Poller()
    poller.register(tick_subscriber, zmq.POLLIN)

    symbol_state_by_symbol: dict[str, SymbolState] = {}
    tick_count_in_window, last_stats_print_time = 0, time.time()
    print("[B] Alpha Engine up", flush=True)

    while not stop_requested.is_set():
        ready_sockets = dict(poller.poll(timeout=RECEIVE_POLL_TIMEOUT_MS))
        if tick_subscriber not in ready_sockets:
            current_time = time.time()
            if current_time - last_stats_print_time >= 10:
                print(f"[B] ticks=0 (0/s)  syms={len(symbol_state_by_symbol)}  "
                      f"top: -  (no ticks received in last "
                      f"{current_time - last_stats_print_time:.0f}s)", flush=True)
                tick_count_in_window, last_stats_print_time = 0, current_time
            continue

        symbol_bytes, message_payload = tick_subscriber.recv_multipart()
        tick_data = json.loads(message_payload)
        latency_ms = (time.time() - tick_data["timestamp"]) * 1000
        symbol = tick_data["symbol"]

        symbol_state = symbol_state_by_symbol.get(symbol)
        if symbol_state is None:
            symbol_state = symbol_state_by_symbol[symbol] = SymbolState()

        update_rolling_windows(symbol_state, tick_data)
        (raw_composite_score, relative_volume, gap_fade_raw_score,
         gap_fade_raw_score_unrestricted) = compute_feature_score(symbol_state, tick_data)

        if symbol_state.last_tick_timestamp == 0:
            symbol_state.smoothed_ema_score = raw_composite_score
            symbol_state.peak_score_amplitude = 0.0
        elif tick_data["timestamp"] > symbol_state.last_tick_timestamp:
            delta_time_seconds = tick_data["timestamp"] - symbol_state.last_tick_timestamp
            ema_alpha = 1.0 - math.exp(-delta_time_seconds / EMA_TIME_CONSTANT_SECONDS)
            symbol_state.smoothed_ema_score = (
                ema_alpha * raw_composite_score
                + (1 - ema_alpha) * symbol_state.smoothed_ema_score)
            peak_decay_factor = math.exp(
                -delta_time_seconds * math.log(2) / PEAK_SCORE_HALF_LIFE_SECONDS)
            symbol_state.peak_score_amplitude = max(
                symbol_state.peak_score_amplitude * peak_decay_factor,
                abs(symbol_state.smoothed_ema_score))
        symbol_state.last_tick_timestamp = max(
            symbol_state.last_tick_timestamp, tick_data["timestamp"])

        if latency_ms > LATENCY_WARNING_MS:
            print(f"[B] high lat={latency_ms:.0f}ms sym={symbol}", flush=True)

        score_publisher.send_multipart([symbol_bytes, json.dumps({
            "symbol": symbol,
            "score": symbol_state.smoothed_ema_score,
            "peak": symbol_state.peak_score_amplitude,
            "relative_volume": relative_volume,
            "gap": gap_fade_raw_score,
            "gap_unrestricted": gap_fade_raw_score_unrestricted,
            "last_traded_price": tick_data["last_traded_price"],
            "timestamp": tick_data["timestamp"],
        }).encode()])

        tick_count_in_window += 1
        current_time = time.time()
        if current_time - last_stats_print_time >= 10:
            top_five_by_absolute_score = sorted(
                symbol_state_by_symbol.items(),
                key=lambda symbol_and_state: -abs(symbol_and_state[1].smoothed_ema_score)
            )[:5]

            def get_last_traded_price(state):
                history = state.last_traded_price_history_30s
                return history[-1][1] if history else 0.0

            summary_line = "  ".join(
                f"{symbol_name}={state.smoothed_ema_score:+.2f}"
                f"(ltp={get_last_traded_price(state):.1f})"
                for symbol_name, state in top_five_by_absolute_score
            ) or "-"
            ticks_per_second = tick_count_in_window / max(current_time - last_stats_print_time, 0.001)
            print(f"[B] ticks={tick_count_in_window} ({ticks_per_second:.0f}/s)  "
                  f"syms={len(symbol_state_by_symbol)}  top: {summary_line}", flush=True)
            tick_count_in_window, last_stats_print_time = 0, current_time

    print("[B] shutting down (signal received)", flush=True)
    for zmq_socket in (tick_subscriber, score_publisher):
        try:
            zmq_socket.setsockopt(zmq.LINGER, 0)
        except Exception:
            pass
    tick_subscriber.close()
    score_publisher.close()
    zmq_context.term()

def determine_signal_modes(composite_score, relative_volume, gap_fade_raw_score=0.0):
    qualifying_modes = []
    if abs(gap_fade_raw_score) >= 2.0:
        qualifying_modes.append("GAP_FADE")
    absolute_score = abs(composite_score)
    if absolute_score >= 8 and relative_volume >= 1.5:
        qualifying_modes.append("MOMENTUM")
    elif absolute_score >= 3 and relative_volume < 2.0:
        qualifying_modes.append("MEAN_REVERSION")
    return qualifying_modes

async def is_entry_cooldown_expired(redis_client, symbol, mode):
    cooldown_key = f"alpha:cd:{symbol}:{mode}"
    was_newly_set = await redis_client.set(
        cooldown_key, "1", ex=COOLDOWN_SECONDS_BY_MODE[mode], nx=True)
    return bool(was_newly_set)

async def is_exit_cooldown_expired(redis_client, symbol, mode):
    cooldown_key = f"alpha:exitcd:{symbol}:{mode}"
    was_newly_set = await redis_client.set(
        cooldown_key, "1", ex=EXIT_ALERT_COOLDOWN_SECONDS, nx=True)
    return bool(was_newly_set)

def build_entry_alert_text(score_data, mode):
    direction = determine_entry_direction(score_data, mode)

    stop_loss_text_by_mode = {
        "MOMENTUM": "1.5%", "MEAN_REVERSION": "0.5%", "GAP_FADE": "gap-based"}
    stop_loss_text = stop_loss_text_by_mode[mode]

    gap_extra_text = (f"  gap={score_data['gap']:+.2f}"
                      if mode == "GAP_FADE" and "gap" in score_data else "")

    return (f"🔔 [{mode}] {score_data['symbol']} {direction}\n"
            f"score={score_data['score']:+.2f}  "
            f"peak={score_data['peak']:.2f}  "
            f"ltp={score_data['last_traded_price']:.2f}{gap_extra_text}\n"
            f"suggested SL: {stop_loss_text}\n"
            f"advisory only — user executes manually")

def build_exit_alert_text(symbol, tracked_position, current_score, current_last_traded_price):
    direction = tracked_position.get(
        "direction", "LONG" if tracked_position.get("score_at_entry", 0.0) > 0 else "SHORT")
    entry_price = tracked_position.get("ltp_entry", 0)
    profit_loss_percent = (
        (current_last_traded_price - entry_price) / entry_price * 100
        if entry_price else 0.0)
    if direction == "SHORT":
        profit_loss_percent = -profit_loss_percent

    mode_text = tracked_position.get("mode", "UNKNOWN")
    score_at_entry = tracked_position.get("score_at_entry", 0.0)

    return (f"⚠️ EXIT — {symbol} ({mode_text} {direction})\n"
            f"score decayed to {current_score:+.2f} "
            f"(entry {score_at_entry:+.2f})\n"
            f"ltp {current_last_traded_price:.2f} "
            f"(entry {entry_price:.2f}, delta {profit_loss_percent:+.2f}%)\n"
            f"consider closing position")

TELEGRAM_BOT_STATE = {"application": None, "entry_keyboard_builder": None, "exit_keyboard_builder": None}

async def initialize_telegram_bot(redis_client_async):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[C] TELEGRAM_TOKEN/CHAT_ID missing — bot disabled, alerts to stdout", flush=True)
        return

    try:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        from telegram.ext import Application, CallbackQueryHandler
    except ImportError as import_error:
        print(f"[C] python-telegram-bot import failed: {import_error}", flush=True)
        return

    async def handle_button_callback(update, context):
        callback_query = update.callback_query
        try:
            await callback_query.answer()
        except Exception:
            pass
        try:
            callback_parts = callback_query.data.split(":", 2)
            action = callback_parts[0]
            symbol = callback_parts[1]
            mode = callback_parts[2] if len(callback_parts) > 2 else ""
            redis_client = context.application.bot_data["redis"]

            original_text_html = callback_query.message.text_html

            if action == "ACK":
                pending_info = await redis_client.get(
                    f"alpha:pending:{callback_query.message.message_id}")
                if pending_info:
                    await redis_client.setex(
                        f"alpha:pos:{symbol}:{mode}", TRACKED_POSITION_TTL_SECONDS, pending_info)
                    await redis_client.delete(
                        f"alpha:pending:{callback_query.message.message_id}")
                    await callback_query.edit_message_text(
                        text=f"{original_text_html}\n\n✅ ACK'd — tracking for exit",
                        parse_mode="HTML", reply_markup=None)
                else:
                    await callback_query.edit_message_text(
                        text=f"{original_text_html}\n\n"
                             f"✅ ACK'd (details expired after 24h)",
                        parse_mode="HTML", reply_markup=None)

            elif action == "SKIP":
                skip_log_key = f"alpha:skip:{time.strftime('%Y%m%d')}"
                await redis_client.lpush(skip_log_key, json.dumps(
                    {"symbol": symbol, "mode": mode, "timestamp": time.time()}))
                await redis_client.expire(skip_log_key, 30 * 24 * 3600)
                await callback_query.edit_message_text(
                    text=f"{original_text_html}\n\n⏭️ SKIPPED (logged)",
                    parse_mode="HTML", reply_markup=None)

            elif action == "DONE":
                await redis_client.delete(f"alpha:pos:{symbol}:{mode}")
                await redis_client.set(
                    f"alpha:cd:{symbol}:{mode}", "1",
                    ex=COOLDOWN_SECONDS_BY_MODE.get(mode, EXIT_ALERT_COOLDOWN_SECONDS))
                await callback_query.edit_message_text(
                    text=f"{original_text_html}\n\n✔️ DONE — position closed",
                    parse_mode="HTML", reply_markup=None)

            elif action == "HOLD":
                await redis_client.setex(
                    f"alpha:exitcd:{symbol}:{mode}", EXIT_ALERT_COOLDOWN_SECONDS * 2, "1")
                await callback_query.edit_message_text(
                    text=f"{original_text_html}\n\n⏳ HOLDING — exit cooldown extended",
                    parse_mode="HTML", reply_markup=None)
        except Exception as button_error:
            print(f"[C] button handler err: {button_error!r}", flush=True)

    try:
        telegram_application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        telegram_application.add_handler(CallbackQueryHandler(handle_button_callback))
        telegram_application.bot_data["redis"] = redis_client_async
        await telegram_application.initialize()
        await telegram_application.start()
        await telegram_application.updater.start_polling(drop_pending_updates=True)

        TELEGRAM_BOT_STATE["application"] = telegram_application
        TELEGRAM_BOT_STATE["entry_keyboard_builder"] = lambda symbol, mode: InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ ACK",  callback_data=f"ACK:{symbol}:{mode}"),
            InlineKeyboardButton("⏭️ SKIP", callback_data=f"SKIP:{symbol}:{mode}"),
        ]])
        TELEGRAM_BOT_STATE["exit_keyboard_builder"] = lambda symbol, mode: InlineKeyboardMarkup([[
            InlineKeyboardButton("✔️ DONE",      callback_data=f"DONE:{symbol}:{mode}"),
            InlineKeyboardButton("⏳ HOLD MORE", callback_data=f"HOLD:{symbol}:{mode}"),
        ]])

        try:
            await telegram_application.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID, text="🟢 Alpha-Decay Lite bot online")
        except Exception as ping_error:
            print(f"[C] startup ping failed: {ping_error!r}", flush=True)

        print(f"[C] Telegram bot up (chat={TELEGRAM_CHAT_ID})", flush=True)
    except Exception as init_error:
        print(f"[C] Telegram bot init failed: {init_error!r} — alerts to stdout only", flush=True)

async def shutdown_telegram_bot():
    telegram_application = TELEGRAM_BOT_STATE["application"]
    if not telegram_application:
        return
    try:
        await telegram_application.updater.stop()
        await telegram_application.stop()
        await telegram_application.shutdown()
    except Exception as shutdown_error:
        print(f"[C] TG shutdown err: {shutdown_error!r}", flush=True)

def determine_entry_direction(score_data, mode):
    if mode == "GAP_FADE":
        return "LONG" if score_data.get("gap", 0.0) > 0 else "SHORT"
    return "LONG" if score_data["score"] > 0 else "SHORT"

async def send_entry_alert(redis_client_async, score_data, mode):
    alert_text = build_entry_alert_text(score_data, mode)
    telegram_application = TELEGRAM_BOT_STATE["application"]

    if telegram_application is None:
        print(f"[TG STUB ENTRY]\n{alert_text}\n---", flush=True)
        return

    try:
        sent_message = await telegram_application.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, text=alert_text,
            reply_markup=TELEGRAM_BOT_STATE["entry_keyboard_builder"](score_data["symbol"], mode))

        pending_position_data = {
            "symbol": score_data["symbol"],
            "mode": mode,
            "direction": determine_entry_direction(score_data, mode),
            "score_at_entry": score_data["score"],
            "peak": score_data["peak"],
            "ltp_entry": score_data["last_traded_price"],
            "entry_timestamp": time.time(),
        }
        await redis_client_async.setex(
            f"alpha:pending:{sent_message.message_id}",
            PENDING_ALERT_TTL_SECONDS, json.dumps(pending_position_data))
    except Exception as send_error:
        print(f"[C] TG entry send failed: {send_error!r}", flush=True)

async def send_exit_alert(score_data, tracked_position, displayed_score):
    alert_text = build_exit_alert_text(
        score_data["symbol"], tracked_position, displayed_score,
        score_data["last_traded_price"])
    telegram_application = TELEGRAM_BOT_STATE["application"]

    if telegram_application is None:
        print(f"[TG STUB EXIT]\n{alert_text}\n---", flush=True)
        return

    try:
        await telegram_application.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, text=alert_text,
            reply_markup=TELEGRAM_BOT_STATE["exit_keyboard_builder"](
                score_data["symbol"], tracked_position.get("mode", "")))
    except Exception as send_error:
        print(f"[C] TG exit send failed: {send_error!r}", flush=True)

async def check_suppression_status(redis_client_async):
    try:
        mute_value = await redis_client_async.get("alpha:mute")
        if mute_value == "1":
            return True, "muted"
        if await asyncio.to_thread(os.path.exists, MUTE_FILE_PATH):
            return True, "muted"
        if not await redis_client_async.get("alpha:hb:B"):
            return True, "engine_dead"
        if not await redis_client_async.get("alpha:hb:data"):
            return True, "no_data"
    except Exception:
        mute_file_exists = await asyncio.to_thread(os.path.exists, MUTE_FILE_PATH)
        return True, ("muted" if mute_file_exists else "redis_down")
    return False, ""

SCORE_RECEIVE_TIMEOUT_SECONDS = 1.0

async def broadcaster_loop():
    zmq_context = zmq.asyncio.Context()
    score_subscriber = zmq_context.socket(zmq.SUB)
    score_subscriber.setsockopt(zmq.RCVHWM, ZMQ_HIGH_WATER_MARK)
    score_subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    score_subscriber.connect(ZMQ_SCORES_ADDRESS)

    stop_requested = asyncio.Event()
    running_loop = asyncio.get_running_loop()
    try:
        for signal_number in (signal.SIGTERM, signal.SIGINT):
            running_loop.add_signal_handler(signal_number, stop_requested.set)
    except NotImplementedError:
        pass

    redis_client_async = redis_asyncio.from_url(
        REDIS_URL, decode_responses=True,
        health_check_interval=30, socket_keepalive=True)

    heartbeat_redis_client_sync = redis.from_url(REDIS_URL, decode_responses=True)
    start_heartbeat_thread("C", heartbeat_redis_client_sync)

    await initialize_telegram_bot(redis_client_async)

    stats_counters = {
        "scores_received": 0, "no_qualifying_mode": 0, "muted": 0,
        "no_data": 0, "engine_dead": 0, "cooldown_blocked": 0,
        "entry_alerts_sent": 0, "exit_alerts_sent": 0, "redis_errors": 0,
    }
    last_stats_print_time = time.time()
    cached_suppression_state = {"is_suppressed": False, "reason": "", "checked_at": 0.0}
    print("[C] Broadcaster up", flush=True)

    try:
        while not stop_requested.is_set():
            try:
                try:
                    symbol_bytes, message_payload = await asyncio.wait_for(
                        score_subscriber.recv_multipart(),
                        timeout=SCORE_RECEIVE_TIMEOUT_SECONDS)
                except asyncio.TimeoutError:
                    current_time = time.time()
                    if current_time - last_stats_print_time >= 15:
                        print(f"[C] {stats_counters} (no scores received in last "
                              f"{current_time - last_stats_print_time:.0f}s)", flush=True)
                        stats_counters = dict.fromkeys(stats_counters, 0)
                        last_stats_print_time = current_time
                    continue

                score_data = json.loads(message_payload)
                stats_counters["scores_received"] += 1

                current_wall_time = time.time()
                if current_wall_time - cached_suppression_state["checked_at"] > MUTE_STATUS_CACHE_SECONDS:
                    is_suppressed, suppression_reason = await check_suppression_status(redis_client_async)
                    cached_suppression_state["is_suppressed"] = is_suppressed
                    cached_suppression_state["reason"] = suppression_reason
                    cached_suppression_state["checked_at"] = current_wall_time

                if cached_suppression_state["is_suppressed"]:
                    reason = cached_suppression_state["reason"]
                    stats_counters[reason] = stats_counters.get(reason, 0) + 1
                else:
                    modes_with_active_position = set()
                    for candidate_mode in COOLDOWN_SECONDS_BY_MODE:
                        try:
                            tracked_position_json = await redis_client_async.get(
                                f"alpha:pos:{score_data['symbol']}:{candidate_mode}")
                        except Exception:
                            tracked_position_json = None
                            stats_counters["redis_errors"] += 1

                        if not tracked_position_json:
                            continue

                        modes_with_active_position.add(candidate_mode)
                        tracked_position = json.loads(tracked_position_json)
                        position_direction = tracked_position.get(
                            "direction",
                            "LONG" if tracked_position.get("score_at_entry", 0.0) > 0 else "SHORT")
                        position_mode = tracked_position.get("mode", candidate_mode)

                        if position_mode == "GAP_FADE":
                            current_exit_metric = score_data.get("gap_unrestricted", 0.0)
                            if position_direction == "LONG":
                                should_exit = current_exit_metric <= EXIT_ALERT_SCORE_THRESHOLD
                            else:
                                should_exit = current_exit_metric >= -EXIT_ALERT_SCORE_THRESHOLD
                        else:
                            current_exit_metric = score_data["score"]
                            if position_direction == "LONG":
                                should_exit = current_exit_metric <= EXIT_ALERT_SCORE_THRESHOLD
                            else:
                                should_exit = current_exit_metric >= -EXIT_ALERT_SCORE_THRESHOLD

                        if should_exit:
                            try:
                                if await is_exit_cooldown_expired(
                                        redis_client_async, score_data["symbol"], position_mode):
                                    await send_exit_alert(
                                        score_data, tracked_position, current_exit_metric)
                                    stats_counters["exit_alerts_sent"] += 1
                            except Exception as exit_error:
                                stats_counters["redis_errors"] += 1
                                print(f"[C] exit err: {exit_error!r}", flush=True)

                    qualifying_modes = determine_signal_modes(
                        score_data["score"], score_data["relative_volume"],
                        score_data.get("gap", 0.0))
                    if not qualifying_modes:
                        stats_counters["no_qualifying_mode"] += 1
                    for signal_mode in qualifying_modes:
                        if signal_mode in modes_with_active_position:
                            stats_counters["position_already_tracked"] = (
                                stats_counters.get("position_already_tracked", 0) + 1)
                            continue
                        try:
                            if not await is_entry_cooldown_expired(
                                    redis_client_async, score_data["symbol"], signal_mode):
                                stats_counters["cooldown_blocked"] += 1
                            else:
                                stats_counters["entry_alerts_sent"] += 1
                                await send_entry_alert(
                                    redis_client_async, score_data, signal_mode)
                        except Exception as entry_error:
                            stats_counters["redis_errors"] += 1
                            print(f"[C] entry err: {entry_error!r}", flush=True)

                current_time = time.time()
                if current_time - last_stats_print_time >= 15:
                    suppression_tag = (f" suppressed={cached_suppression_state['reason']}"
                                       if cached_suppression_state["is_suppressed"] else "")
                    print(f"[C] {stats_counters}{suppression_tag}", flush=True)
                    stats_counters = dict.fromkeys(stats_counters, 0)
                    last_stats_print_time = current_time
            except asyncio.CancelledError:
                raise
            except Exception as loop_error:
                print(f"[C] loop err: {loop_error!r}", flush=True)
                await asyncio.sleep(0.1)
    finally:
        await shutdown_telegram_bot()
        try:
            await redis_client_async.aclose()
        except Exception as redis_close_error:
            print(f"[C] redis close err: {redis_close_error!r}", flush=True)

        try:
            score_subscriber.setsockopt(zmq.LINGER, 0)
        except Exception:
            pass
        try:
            score_subscriber.close()
        except Exception as socket_close_error:
            print(f"[C] score_subscriber close err: {socket_close_error!r}", flush=True)
        try:
            zmq_context.term()
        except Exception as zmq_close_error:
            print(f"[C] zmq context term err: {zmq_close_error!r}", flush=True)

def run_broadcaster_process():
    asyncio.run(broadcaster_loop())

IPC_SOCKET_FILE_PATHS = [
    address[len("ipc://"):]
    for address in (ZMQ_TICKS_ADDRESS, ZMQ_SCORES_ADDRESS)
    if address.startswith("ipc://")
]
MAX_RESTARTS_ALLOWED_PER_WINDOW = 6
RESTART_COUNTING_WINDOW_SECONDS = 3600

def cleanup_ipc_socket_files():
    for socket_path in IPC_SOCKET_FILE_PATHS:
        try:
            if os.path.exists(socket_path):
                os.remove(socket_path)
                print(f"[launcher] removed stale ipc socket: {socket_path}", flush=True)
        except Exception as cleanup_error:
            print(f"[launcher] ipc cleanup warn ({socket_path}): {cleanup_error!r}", flush=True)

def spawn_all_processes():
    process_list = [
        Process(target=data_factory,           name="A-DataFactory"),
        Process(target=alpha_engine,           name="B-AlphaEngine"),
        Process(target=run_broadcaster_process, name="C-Broadcaster"),
    ]
    for process in process_list:
        process.start()
    return process_list

def shutdown_all_processes(process_list, shutdown_reason):
    print(f"\n[launcher] shutdown: {shutdown_reason}", flush=True)
    for process in process_list:
        if process.is_alive():
            process.terminate()

    shutdown_deadline = time.time() + 5
    for process in process_list:
        remaining_seconds = max(0.1, shutdown_deadline - time.time())
        process.join(timeout=remaining_seconds)

    for process in process_list:
        if process.is_alive():
            print(f"[launcher] force-kill {process.name}", flush=True)
            process.kill()
            process.join(timeout=2)

    cleanup_ipc_socket_files()

if __name__ == "__main__":
    cleanup_ipc_socket_files()
    running_processes = spawn_all_processes()

    stop_event = threading.Event()
    for signal_number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signal_number, lambda *_args: stop_event.set())

    recent_restart_timestamps = deque()

    try:
        while not stop_event.is_set():
            dead_processes = [process for process in running_processes if not process.is_alive()]

            if dead_processes:
                exit_codes_by_process_name = {
                    process.name: process.exitcode for process in dead_processes}

                has_fatal_config_error = any(
                    code == EXIT_CODE_FATAL_CONFIG for code in exit_codes_by_process_name.values())
                if has_fatal_config_error:
                    shutdown_all_processes(
                        running_processes,
                        f"fatal config error, not retrying: {exit_codes_by_process_name}")
                    print("[launcher] FATAL: a process exited due to invalid configuration "
                          "or credentials (see the error above). Fix .env and restart manually "
                          "-- not retrying automatically.", flush=True)
                    sys.exit(1)

                is_planned_restart = any(
                    code == EXIT_CODE_PLANNED_RESTART for code in exit_codes_by_process_name.values())
                shutdown_reason = ("planned restart (JWT refresh)" if is_planned_restart
                                  else f"process died: {exit_codes_by_process_name}")
                shutdown_all_processes(running_processes, shutdown_reason)

                current_time = time.time()
                recent_restart_timestamps.append(current_time)
                while (recent_restart_timestamps
                       and current_time - recent_restart_timestamps[0] > RESTART_COUNTING_WINDOW_SECONDS):
                    recent_restart_timestamps.popleft()

                if len(recent_restart_timestamps) > MAX_RESTARTS_ALLOWED_PER_WINDOW:
                    print(f"[launcher] FATAL: {len(recent_restart_timestamps)} restarts in the "
                          f"last {RESTART_COUNTING_WINDOW_SECONDS / 3600:.0f}h -- crash loop "
                          f"suspected (check credentials/.env/network). "
                          f"Exiting without further retry.", flush=True)
                    sys.exit(1)

                print("[launcher] respawning all processes...", flush=True)
                time.sleep(2)
                running_processes = spawn_all_processes()
                continue

            time.sleep(1)

        shutdown_all_processes(running_processes, "signal received")
    except KeyboardInterrupt:
        shutdown_all_processes(running_processes, "KeyboardInterrupt")
