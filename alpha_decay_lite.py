"""
Alpha-Decay Lite -- signal-only NSE cash equity alerting engine.
Real-time, per-tick pipeline:  Angel WS (mode 3)  ->  ZMQ  ->  9 calculators + EMA  ->  Telegram
Advisory only.  No order placement.

NOTE ON NAMING: every variable and function in this file uses a full,
descriptive name (no single-letter or cryptic abbreviations) so that
the code is self-explanatory without needing to cross-reference this
comment block or external documentation.
"""
import os
import sys
import time
import signal
import threading

# Force IST for time.localtime() calls (fast C-level path + correct timezone
# regardless of what timezone the server itself is configured with).
#
# IMPORTANT PLATFORM NOTE: time.tzset() is documented as Unix-only
# (Python docs: "Availability: Unix"). On Windows, os.environ["TZ"]
# alone does NOT affect time.localtime()'s output, because Windows'
# C runtime does not consult the TZ environment variable the way Unix
# libc does, and time.tzset() itself does not exist there. This
# deployment targets Linux VPS only (see README/deploy docs), so this
# is not a practical issue for actual usage -- but if this script is
# ever run on Windows by mistake, calculate_gap_fade and
# calculate_session_weight's time-of-day windows would silently use
# the WRONG timezone with no error raised. The check below fails fast
# with a clear message instead of producing silently-wrong signals.
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

# ============================================================
# CONFIGURATION CONSTANTS
# ============================================================
ZMQ_TICKS_ADDRESS   = "ipc:///tmp/alpha_ticks.ipc"    # Process A -> Process B
ZMQ_SCORES_ADDRESS  = "ipc:///tmp/alpha_scores.ipc"   # Process B -> Process C
REDIS_URL            = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MUTE_FILE_PATH       = os.getenv("MUTE_FILE", "/tmp/alpha_mute.flag")
TRADING_UNIVERSE     = os.getenv("UNIVERSE", "RELIANCE,TCS,HDFCBANK").split(",")
SCRIP_MASTER_URL     = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

TELEGRAM_BOT_TOKEN   = os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Angel SDK mode-3 SNAP_QUOTE has been reported to swap best_5_buy_data
# and best_5_sell_data at the output layer on some SDK versions. Set
# ANGEL_BIDASK_SWAPPED=1 in .env ONLY after running diag_bidask.py
# confirms the swap on your specific SDK version. Setting this wrong
# would ACTIVELY break the spread filter.
ANGEL_BID_ASK_IS_SWAPPED = os.getenv("ANGEL_BIDASK_SWAPPED", "0").strip() == "1"

SPREAD_RATIO_MAX            = 0.0015   # 0.15% -- above this, score is gated to 0
EMA_TIME_CONSTANT_SECONDS   = 1.0      # tau for the score smoother (calculator #9)

# Cooldowns are NOT rate limits. This system is advisory-only -- the user
# decides which alerts to act on, so every qualifying signal is delivered.
# Cooldowns exist only to stop the SAME signal (symbol + mode) from firing
# again and again while the score stays inside its trigger band.
COOLDOWN_SECONDS_BY_MODE = {"MOMENTUM": 300, "MEAN_REVERSION": 45, "GAP_FADE": 180}

ZMQ_HIGH_WATER_MARK      = 10000   # max queued messages per ZMQ socket before dropping
LATENCY_WARNING_MS       = 100     # log a warning if tick-to-score latency exceeds this
MUTE_STATUS_CACHE_SECONDS = 1.0    # how long Process C caches the mute/heartbeat check
RVOL_WARMUP_SECONDS      = 300     # need 5+ minutes of history before RVOL is meaningful
PEAK_SCORE_HALF_LIFE_SECONDS = 60.0  # how fast the "peak score" amplitude decays over time
AUTO_RESTART_AFTER_HOURS = float(os.getenv("AUTO_RESTART_HOURS", "20"))  # < Angel JWT ~24-28h expiry

# Process exit codes the launcher inspects to decide how to react.
# 0 (a bare `return`) and any other unrecognized code are treated as an
# unplanned crash -> respawn and count against the crash-loop breaker.
EXIT_CODE_PLANNED_RESTART = 2   # scheduled JWT-refresh self-exit -> respawn immediately, no penalty
EXIT_CODE_FATAL_CONFIG    = 3   # missing/invalid credentials or config -> NOT retryable; stop immediately
                                # instead of burning through the crash-loop budget on an error that
                                # will not fix itself without human intervention (editing .env)

# ---- Kill switches (heartbeats) ----
PROCESS_HEARTBEAT_TTL_SECONDS      = 3   # process-alive heartbeat TTL (kill switch #2)
DATA_FLOW_HEARTBEAT_TTL_SECONDS    = 5   # data-is-flowing heartbeat TTL (kill switch #1)
HEARTBEAT_WRITE_INTERVAL_SECONDS   = 1

# ---- Position tracking (for ACK'd entries -> exit alerts) ----
EXIT_ALERT_SCORE_THRESHOLD   = 2.0        # |score| <= this -> fire an exit alert
EXIT_ALERT_COOLDOWN_SECONDS  = 300        # minimum time between exit alerts for the same symbol
TRACKED_POSITION_TTL_SECONDS = 6 * 3600   # spec requirement: auto-cleanup stale positions after 6h+
PENDING_ALERT_TTL_SECONDS    = 24 * 3600  # how long an un-ACK'd alert stays actionable in Redis


def clip_value(value, minimum, maximum):
    """Clamp value into the inclusive range [minimum, maximum]."""
    return max(minimum, min(maximum, value))


# ============================================================
# AMORTIZED-O(1) ROLLING WINDOW HELPER
# ============================================================
class RollingWindowSum:
    """Maintains a running sum of (timestamp, value) pairs over a trailing
    time window. Both add() and the running total are O(1) amortized:
    each call to add() appends one new entry and evicts any entries that
    have aged out of the window, so no full re-scan is ever needed."""
    __slots__ = ("window_seconds", "entries", "running_sum")

    def __init__(self, window_seconds):
        self.window_seconds = window_seconds
        self.entries = deque()      # each entry is (timestamp, value)
        self.running_sum = 0.0

    def add(self, timestamp, value):
        """Add a new (timestamp, value) sample and evict anything older
        than window_seconds. IMPORTANT: call this on every tick, even with
        value=0.0, so that eviction always runs and old data never lingers
        during a quiet stretch with no meaningful contribution."""
        self.entries.append((timestamp, value))
        self.running_sum += value
        eviction_cutoff = timestamp - self.window_seconds
        while self.entries and self.entries[0][0] < eviction_cutoff:
            self.running_sum -= self.entries.popleft()[1]

    def span_seconds(self):
        """Time span currently covered by the window's oldest and newest
        entries. Used to detect 'not enough history yet' (warmup) cases."""
        if len(self.entries) < 2:
            return 0.0
        return self.entries[-1][0] - self.entries[0][0]


# ============================================================
# HEARTBEAT HELPER (used by Process A, B, and C)
# ============================================================
def start_heartbeat_thread(heartbeat_name, redis_client, additional_check=None):
    """Starts a background daemon thread that refreshes the Redis key
    alpha:hb:{heartbeat_name} every HEARTBEAT_WRITE_INTERVAL_SECONDS,
    with a TTL of PROCESS_HEARTBEAT_TTL_SECONDS. If additional_check is
    provided, it must be a zero-argument callable returning
    (extra_heartbeat_name, is_currently_alive); when is_currently_alive
    is True, alpha:hb:{extra_heartbeat_name} is also refreshed, using
    DATA_FLOW_HEARTBEAT_TTL_SECONDS as its TTL."""

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


# ============================================================
# PROCESS A: DATA FACTORY (Angel SmartWebSocketV2, snap-quote mode 3)
# ============================================================
def resolve_nse_equity_tokens(symbol_list):
    """Downloads Angel's scrip master JSON and maps each requested NSE
    equity symbol (e.g. 'RELIANCE') to its numeric exchange token, which
    is what the WebSocket subscribe call requires (not the symbol name).
    Returns a dict of {token: symbol} for every symbol that was found."""
    start_time = time.time()
    print("[A] Downloading scrip master (~4 MB)...", flush=True)
    scrip_master_list = requests.get(SCRIP_MASTER_URL, timeout=60).json()
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
    """Process A entry point. Logs in to Angel SmartAPI, resolves the
    trading universe to exchange tokens, opens the market-data WebSocket
    in snap-quote mode 3, and publishes every incoming tick over ZMQ for
    Process B to consume."""
    from SmartApi import SmartConnect
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
    import pyotp

    angel_api_key      = os.getenv("ANGEL_API_KEY")
    angel_client_code  = os.getenv("ANGEL_CLIENT_CODE")
    angel_password     = os.getenv("ANGEL_PASSWORD")        # 4-digit MPIN, NOT the web login password
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

    # JWT auto-refresh: schedule a clean self-exit before the token expires
    # (~24-28h typical lifetime). The launcher's respawn logic restarts this
    # process automatically after the exit.
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

    zmq_context = zmq.Context()   # fresh per-process context, avoids fork-inherited state
    tick_publisher = zmq_context.socket(zmq.PUB)
    tick_publisher.setsockopt(zmq.SNDHWM, ZMQ_HIGH_WATER_MARK)
    tick_publisher.bind(ZMQ_TICKS_ADDRESS)
    time.sleep(1.5)   # give SUB peers time to connect (mitigates ZMQ "slow joiner" packet loss)
    print(f"[A] Data Factory up, {len(token_list)} tokens mode=3", flush=True)

    # Heartbeats: kill switch #1 (is data flowing) + kill switch #2's counterpart (process alive).
    heartbeat_redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    last_tick_received_at = {"timestamp": 0.0}

    def check_data_is_flowing():
        seconds_since_last_tick = time.time() - last_tick_received_at["timestamp"]
        return "data", seconds_since_last_tick < DATA_FLOW_HEARTBEAT_TTL_SECONDS

    start_heartbeat_thread("A", heartbeat_redis_client, additional_check=check_data_is_flowing)

    # The SDK's default max_retry_attempt=1 means only one reconnect attempt
    # before giving up entirely. Bump these so transient network hiccups
    # don't require a manual restart. Falls back to defaults if the
    # installed SDK version doesn't accept these keyword arguments.
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
        last_tick_received_at["timestamp"] = time.time()

        best_5_buy_levels  = market_data_message.get("best_5_buy_data") or []
        best_5_sell_levels = market_data_message.get("best_5_sell_data") or []
        # If the SDK is swapping the buy/sell labels (confirmed via diag_bidask.py),
        # exchange them here to compensate.
        if ANGEL_BID_ASK_IS_SWAPPED:
            best_5_buy_levels, best_5_sell_levels = best_5_sell_levels, best_5_buy_levels

        tick_data = {
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
        }
        try:
            tick_publisher.send_multipart(
                [symbol.encode(), json.dumps(tick_data).encode()], zmq.DONTWAIT)
        except zmq.Again:
            pass   # drop this tick under backpressure (publisher high-water-mark reached)

    websocket_client.on_data = on_tick_received
    websocket_client.on_open = lambda _websocket: websocket_client.subscribe(
        "adl", 3, [{"exchangeType": 1, "tokens": token_list}])
    websocket_client.on_error = lambda _websocket, error: print(
        f"[A] WS error: {error}", flush=True)
    websocket_client.on_close = lambda _websocket: print(
        "[A] WS closed (SDK will reconnect)", flush=True)
    websocket_client.connect()


# ============================================================
# PROCESS B: ALPHA ENGINE
# ============================================================
@dataclass
class SymbolState:
    """Per-symbol rolling state maintained by the Alpha Engine. One
    instance of this exists for every symbol currently being tracked."""
    volume_sum_last_60_seconds: RollingWindowSum = field(
        default_factory=lambda: RollingWindowSum(60))
    volume_sum_last_20_minutes: RollingWindowSum = field(
        default_factory=lambda: RollingWindowSum(1200))
    # Trade-classified (Lee-Ready) net and gross volume over a 30s window,
    # shared by the aggressive-buy calculator and the absorption calculator.
    aggressive_signed_volume_30s: RollingWindowSum = field(
        default_factory=lambda: RollingWindowSum(30))
    aggressive_absolute_volume_30s: RollingWindowSum = field(
        default_factory=lambda: RollingWindowSum(30))
    last_traded_price_history_30s: deque = field(default_factory=deque)   # (timestamp, price)
    last_cumulative_volume: int = -1          # -1 = uninitialized (first-tick baseline not yet set)
    last_traded_price: float = 0.0            # used by the tick-rule fallback classifier
    smoothed_ema_score: float = 0.0
    last_tick_timestamp: float = 0.0
    peak_score_amplitude: float = 0.0


# ---- 9 calculators. Each is amortized O(1) per tick. ----

def calculate_vwap_distance(_symbol_state, tick_data):
    """Calculator #1: percentage distance of last-traded-price from the
    daily VWAP. Because the daily VWAP is cumulative, its 60-second slope
    is near-zero by mid-afternoon -- distance from it is the meaningful
    mean-reversion signal, not its slope."""
    if not tick_data["daily_vwap"] or not tick_data["last_traded_price"]:
        return 0.0
    percent_distance = ((tick_data["last_traded_price"] - tick_data["daily_vwap"])
                         / tick_data["daily_vwap"] * 100)
    return clip_value(percent_distance * 5.0, -2.0, 2.0)   # 0.4% away -> +-2.0 (capped)


def calculate_relative_volume(symbol_state, _tick_data):
    """Calculator #2: RVOL = last-60-seconds volume divided by the average
    per-minute volume of the PRIOR baseline minutes (the most recent 60
    seconds is explicitly excluded from the baseline to avoid
    self-inclusion bias). Returns a neutral 1.0 during the first 5
    minutes of history (warmup), to avoid false spikes caused by a
    near-empty baseline.

    IMPORTANT: the baseline divisor is the ACTUAL number of elapsed
    baseline minutes so far (capped at 19, since the window holds at
    most 20 minutes and 1 of those is the excluded most-recent-60s),
    NOT a hardcoded 19.0. Dividing by a fixed 19.0 while the window has
    only been accumulating for, say, 5-6 minutes would understate the
    baseline volume by roughly 3-4x, which inflates RVOL by the same
    factor and can spuriously satisfy the MOMENTUM mode's rvol >= 1.5
    threshold on perfectly ordinary volume. Verified via simulation: at
    5 minutes of steady (non-spiking) volume, the fixed-19.0 version
    reported rvol=4.75 where the true value is 1.00; the elapsed-minutes
    version correctly reports 1.00 throughout."""
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
    """Calculator #3: rolling 30-second NET aggressive buy/sell pressure,
    built from Lee-Ready (with tick-rule fallback) trade classification.
    Using a rolling window instead of a single tick's classification
    dampens noise considerably."""
    gross_classified_volume = symbol_state.aggressive_absolute_volume_30s.running_sum
    if gross_classified_volume < 100:
        return 0.0
    net_classified_volume = symbol_state.aggressive_signed_volume_30s.running_sum
    return clip_value(net_classified_volume / gross_classified_volume * 2.0, -2.0, 2.0)


def calculate_absorption(symbol_state, tick_data):
    """Calculator #4: trade-classified, SYMMETRIC absorption signal.
       +1.5 : heavy net SELL volume while price stays flat -> sellers are
              being absorbed (BULLISH signal -- hidden buyer underneath)
       -1.5 : heavy net BUY volume while price stays flat -> buyers are
              being absorbed (BEARISH trap -- hidden seller overhead)
        0.0 : neither condition met"""
    price_history = symbol_state.last_traded_price_history_30s
    if len(price_history) < 5 or not tick_data["last_traded_price"]:
        return 0.0
    lowest_price_in_window = min(price for _, price in price_history)
    highest_price_in_window = max(price for _, price in price_history)
    price_range_ratio = ((highest_price_in_window - lowest_price_in_window)
                          / tick_data["last_traded_price"])
    if price_range_ratio > 0.001:
        return 0.0   # price is not flat enough for this to count as absorption

    if symbol_state.aggressive_signed_volume_30s.span_seconds() < 10:
        return 0.0   # not enough classified-volume history yet

    gross_classified_volume = symbol_state.aggressive_absolute_volume_30s.running_sum
    if gross_classified_volume < 100:
        return 0.0

    net_to_gross_ratio = (symbol_state.aggressive_signed_volume_30s.running_sum
                          / gross_classified_volume)
    if net_to_gross_ratio < -0.4:
        return 1.5    # heavy net selling, absorbed -> bullish
    if net_to_gross_ratio > 0.4:
        return -1.5   # heavy net buying, absorbed -> bearish trap
    return 0.0


def calculate_book_imbalance(_symbol_state, tick_data):
    """Calculator #5: order-book depth imbalance across the 5 levels
    that Angel provides in total_buy_quantity / total_sell_quantity."""
    total_buy_quantity = tick_data["total_buy_quantity"]
    total_sell_quantity = tick_data["total_sell_quantity"]
    combined_quantity = total_buy_quantity + total_sell_quantity
    if not combined_quantity:
        return 0.0
    imbalance_ratio = (total_buy_quantity - total_sell_quantity) / combined_quantity
    return clip_value(imbalance_ratio * 2, -2.0, 2.0)


def calculate_spread_ratio(_symbol_state, tick_data):
    """Calculator #6: bid-ask spread as a fraction of last-traded-price.
    This is the input to the spread FILTER (see SPREAD_RATIO_MAX), not a
    directional score contribution by itself."""
    if (tick_data["last_traded_price"] and tick_data["best_bid_price"]
            and tick_data["best_ask_price"]):
        return ((tick_data["best_ask_price"] - tick_data["best_bid_price"])
                / tick_data["last_traded_price"])
    return 1.0   # treat missing quote data as "spread too wide" (fail safe)


def calculate_gap_fade(_symbol_state, tick_data):
    """Calculator #7: gap-fade signal, active only in the 9:15:00-9:29:59
    IST window. Returns the negated percentage gap between the day's open
    and the previous close, so a positive result means 'fade the gap by
    going long' and a negative result means 'fade by going short'."""
    local_time = time.localtime(tick_data["timestamp"])
    is_within_gap_window = (local_time.tm_hour == 9
                            and 15 <= local_time.tm_min < 30)
    if not is_within_gap_window or not tick_data["previous_close_price"]:
        return 0.0
    gap_percent = ((tick_data["open_price"] - tick_data["previous_close_price"])
                   / tick_data["previous_close_price"]) * 100
    return clip_value(-gap_percent, -3.0, 3.0)


def calculate_session_weight(_symbol_state, tick_data):
    """Calculator #8: multiplier applied to the composite score based on
    which part of the trading session the current tick falls in.
       OPEN_VOL   (09:15-09:45 IST) -> 1.2  (highest signal quality)
       CLOSE_HOUR (14:45-15:30 IST) -> 1.1  (institutional square-off flow)
       MID_QUIET  (everything else) -> 0.8  (lower conviction, dampen)"""
    local_time = time.localtime(tick_data["timestamp"])
    hour, minute = local_time.tm_hour, local_time.tm_min
    if hour == 9 and minute < 45:
        return 1.2
    if (hour == 14 and minute >= 45) or (hour == 15 and minute <= 30):
        return 1.1
    return 0.8
# Calculator #9 is the EMA smoother, applied inline inside alpha_engine()'s main loop.


def compute_feature_score(symbol_state, tick_data):
    """Runs all 9 calculators and combines them into one composite score
    in the range [-10, +10]. Returns (composite_score, relative_volume,
    gap_fade_raw_score) -- the latter two are needed downstream by
    Process C's mode-selection logic."""
    relative_volume = calculate_relative_volume(symbol_state, tick_data)

    spread_ratio = calculate_spread_ratio(symbol_state, tick_data)
    if spread_ratio > SPREAD_RATIO_MAX:
        # Spread too wide to trust this quote -- gate the score to zero,
        # but still return the real relative_volume (it's independently useful).
        return 0.0, relative_volume, 0.0

    vwap_distance_score      = calculate_vwap_distance(symbol_state, tick_data)
    aggressive_buy_score     = calculate_aggressive_buy_pressure(symbol_state, tick_data)
    absorption_score         = calculate_absorption(symbol_state, tick_data)
    book_imbalance_score     = calculate_book_imbalance(symbol_state, tick_data)
    gap_fade_raw_score       = calculate_gap_fade(symbol_state, tick_data)
    session_weight           = calculate_session_weight(symbol_state, tick_data)

    # Combine aggressive-buy and absorption: if they agree in sign, dampen
    # the redundancy (same-direction confirmation shouldn't double-count);
    # if they disagree, add them (net directional pressure).
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
    return composite_score, relative_volume, gap_fade_raw_score


def classify_trade_direction(tick_data, previous_last_traded_price):
    """Lee-Ready trade classification with a tick-rule fallback.
    Returns +1 for an aggressive buy, -1 for an aggressive sell, or 0 if
    the trade cannot be classified (price exactly at midpoint and no
    change from the previous tick)."""
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
    """Updates every rolling window inside symbol_state for the current
    tick: raw volume windows, trade-classified aggression windows, and
    the short last-traded-price history used by the absorption calculator."""
    current_timestamp = tick_data["timestamp"]

    if symbol_state.last_cumulative_volume < 0:
        # First tick ever seen for this symbol: establish the baseline
        # without treating the entire day's cumulative volume as a
        # single-tick spike.
        symbol_state.last_cumulative_volume = tick_data["cumulative_day_volume"]
        delta_volume = 0
    elif tick_data["cumulative_day_volume"] >= symbol_state.last_cumulative_volume:
        delta_volume = (tick_data["cumulative_day_volume"]
                        - symbol_state.last_cumulative_volume)
        symbol_state.last_cumulative_volume = tick_data["cumulative_day_volume"]
    else:
        # The new cumulative volume is LOWER than the watermark we already
        # have. Since cumulative_day_volume only ever increases over the
        # trading day, this can only mean the tick arrived out of order
        # (a network-delayed older packet reordered behind a newer one --
        # common with UDP-style delivery and WebSocket reconnects).
        #
        # Treating this as a normal update (the old behavior: delta =
        # max(0, new - old), then overwrite the watermark with the SMALLER
        # value) corrupts the running watermark downward. The next
        # in-order tick would then compute its delta against that too-low
        # watermark, double-counting volume that was already recorded.
        # Example (verified via simulation): watermark=1000, an
        # out-of-order tick reports 980 (delta=0, but watermark WRONGLY
        # drops to 980), then the next real tick reports 1010 -- computing
        # delta = 1010-980 = 30 instead of the correct 1010-1000 = 10,
        # a 3x volume overstatement that can spuriously inflate RVOL and
        # trigger a false MOMENTUM alert.
        #
        # Fix: treat cumulative_day_volume as monotonically non-decreasing.
        # An out-of-order/stale tick contributes zero volume and the
        # watermark is left untouched, so it cannot corrupt future deltas.
        delta_volume = 0

    symbol_state.volume_sum_last_60_seconds.add(current_timestamp, delta_volume)
    symbol_state.volume_sum_last_20_minutes.add(current_timestamp, delta_volume)

    # Trade classification feeds both the aggressive-buy calculator and the
    # absorption calculator (shared infrastructure). IMPORTANT: .add() is
    # called on every single tick, even when this tick contributes zero,
    # because the eviction of old entries happens INSIDE add(). If we only
    # called add() when there was an aggressive trade, a quiet stretch with
    # no aggressive trades would mean eviction never runs, and stale
    # minutes-old volume would keep being read as if it were "the last 30
    # seconds" -- a real bug that was caught and fixed.
    trade_direction = classify_trade_direction(tick_data, symbol_state.last_traded_price)
    symbol_state.last_traded_price = tick_data["last_traded_price"]

    trade_is_classified = trade_direction != 0 and delta_volume > 0
    signed_volume_contribution = (trade_direction * delta_volume) if trade_is_classified else 0.0
    absolute_volume_contribution = delta_volume if trade_is_classified else 0.0
    symbol_state.aggressive_signed_volume_30s.add(current_timestamp, signed_volume_contribution)
    symbol_state.aggressive_absolute_volume_30s.add(current_timestamp, absolute_volume_contribution)

    price_history = symbol_state.last_traded_price_history_30s
    price_history.append((current_timestamp, tick_data["last_traded_price"]))
    price_history_cutoff = current_timestamp - 30
    while price_history and price_history[0][0] < price_history_cutoff:
        price_history.popleft()


def alpha_engine():
    """Process B entry point. Consumes ticks published by Process A,
    updates each symbol's rolling state, computes the 9-calculator
    composite score, applies the EMA smoother, and publishes the
    resulting score for Process C to broadcast."""
    zmq_context = zmq.Context()   # fresh per-process context

    tick_subscriber = zmq_context.socket(zmq.SUB)
    tick_subscriber.setsockopt(zmq.RCVHWM, ZMQ_HIGH_WATER_MARK)
    tick_subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    tick_subscriber.connect(ZMQ_TICKS_ADDRESS)

    score_publisher = zmq_context.socket(zmq.PUB)
    score_publisher.setsockopt(zmq.SNDHWM, ZMQ_HIGH_WATER_MARK)
    score_publisher.bind(ZMQ_SCORES_ADDRESS)

    heartbeat_redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    start_heartbeat_thread("B", heartbeat_redis_client)   # kill switch #2

    # multiprocessing.Process.terminate() sends SIGTERM to this process.
    # Without our own handler, Python's default disposition for SIGTERM
    # is immediate termination -- the launcher's terminate()/join() would
    # still "work" from the launcher's point of view, but this process
    # gets no chance to close its sockets cleanly. Registering a handler
    # here means the process instead breaks out of its main loop and exits
    # via a normal return, which is a cleaner shutdown.
    stop_requested = threading.Event()

    def handle_stop_signal(_signum, _frame):
        stop_requested.set()

    signal.signal(signal.SIGTERM, handle_stop_signal)
    signal.signal(signal.SIGINT, handle_stop_signal)

    # A plain, unconditional recv_multipart() blocks forever if Process A
    # stops sending ticks (market closed, WS disconnected, etc). While
    # blocked, this loop can never reach its periodic stats-print check,
    # so the console goes completely silent with no indication of why --
    # it looks indistinguishable from a crash. Using a Poller with a
    # timeout means the loop always wakes up at least once per
    # RECEIVE_POLL_TIMEOUT_MS, whether or not a tick actually arrived, so
    # stats keep printing (showing ticks=0) and the stop signal is checked
    # promptly instead of only being handled after the next tick shows up.
    RECEIVE_POLL_TIMEOUT_MS = 1000
    poller = zmq.Poller()
    poller.register(tick_subscriber, zmq.POLLIN)

    symbol_state_by_symbol: dict[str, SymbolState] = {}
    tick_count_in_window, last_stats_print_time = 0, time.time()
    print("[B] Alpha Engine up", flush=True)

    while not stop_requested.is_set():
        ready_sockets = dict(poller.poll(timeout=RECEIVE_POLL_TIMEOUT_MS))
        if tick_subscriber not in ready_sockets:
            # No tick arrived within the poll timeout. Still run the
            # periodic stats print below so silence is visible as
            # "ticks=0", not indistinguishable from a hang.
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
        raw_composite_score, relative_volume, gap_fade_raw_score = compute_feature_score(
            symbol_state, tick_data)

        # --- Calculator #9: EMA smoother, plus time-based peak-amplitude decay ---
        if symbol_state.last_tick_timestamp == 0:
            # First tick ever for this symbol: initialize directly, no smoothing yet.
            symbol_state.smoothed_ema_score = raw_composite_score
            symbol_state.peak_score_amplitude = abs(raw_composite_score)
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
        # else: this tick has the same timestamp as the previous one (a
        # batched/duplicate tick) -- preserve the previous smoothed score
        # and peak amplitude unchanged.
        symbol_state.last_tick_timestamp = tick_data["timestamp"]

        if latency_ms > LATENCY_WARNING_MS:
            print(f"[B] high lat={latency_ms:.0f}ms sym={symbol}", flush=True)

        score_publisher.send_multipart([symbol_bytes, json.dumps({
            "symbol": symbol,
            "score": symbol_state.smoothed_ema_score,
            "peak": symbol_state.peak_score_amplitude,
            "relative_volume": relative_volume,
            "gap": gap_fade_raw_score,
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
    tick_subscriber.close()
    score_publisher.close()
    # Closing individual sockets releases their file descriptors, but the
    # underlying zmq.Context (a C++-level I/O thread pool) is separate and
    # was never explicitly torn down. term() blocks until the context's
    # I/O threads have shut down cleanly; without it, those threads (and
    # any lingering socket state) could remain alive briefly after this
    # function returns, which is wasted cleanup work for a process that's
    # about to exit anyway but is cheap and correct to do properly.
    zmq_context.term()


# ============================================================
# PROCESS C: BROADCASTER + TELEGRAM BOT
# ============================================================
def determine_signal_mode(composite_score, relative_volume, gap_fade_raw_score=0.0):
    """Decides which alert mode (if any) the current score/volume/gap
    combination qualifies for. Checked in priority order:
       GAP_FADE       : |gap_fade_raw_score| >= 2.0
                         (only nonzero 9:15-9:30 IST, via calculate_gap_fade's gating)
       MOMENTUM       : |composite_score| >= 8 and relative_volume >= 1.5
       MEAN_REVERSION : 3 <= |composite_score| < 8 and relative_volume < 2.0
    Returns None if none of the above conditions are met."""
    if abs(gap_fade_raw_score) >= 2.0:
        return "GAP_FADE"
    absolute_score = abs(composite_score)
    if absolute_score >= 8 and relative_volume >= 1.5:
        return "MOMENTUM"
    if 3 <= absolute_score < 8 and relative_volume < 2.0:
        return "MEAN_REVERSION"
    return None


async def is_entry_cooldown_expired(redis_client, symbol, mode):
    """Atomically sets a cooldown key for (symbol, mode) if and only if
    it doesn't already exist. Returns True (cooldown "expired"/available)
    only on the call that successfully sets the key -- every subsequent
    call within the cooldown window returns False."""
    cooldown_key = f"alpha:cd:{symbol}:{mode}"
    was_newly_set = await redis_client.set(
        cooldown_key, "1", ex=COOLDOWN_SECONDS_BY_MODE[mode], nx=True)
    return bool(was_newly_set)


async def is_exit_cooldown_expired(redis_client, symbol):
    """Same pattern as is_entry_cooldown_expired, but for exit alerts,
    which use a single fixed cooldown regardless of mode."""
    cooldown_key = f"alpha:exitcd:{symbol}"
    was_newly_set = await redis_client.set(
        cooldown_key, "1", ex=EXIT_ALERT_COOLDOWN_SECONDS, nx=True)
    return bool(was_newly_set)


def build_entry_alert_text(score_data, mode):
    """Builds the human-readable text for an entry alert message."""
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
    """Builds the human-readable text for an exit alert message, fired
    when an ACK'd position's score has moved back toward (or through)
    the exit threshold."""
    # Prefer the direction saved at entry time (correct for every mode,
    # including GAP_FADE). Fall back to deriving it from score_at_entry
    # only for positions created before this field existed -- using
    # .get() for score_at_entry too, so a malformed/partial Redis record
    # degrades to a default instead of raising KeyError here.
    direction = tracked_position.get(
        "direction", "LONG" if tracked_position.get("score_at_entry", 0.0) > 0 else "SHORT")
    entry_price = tracked_position.get("ltp_entry", 0)
    profit_loss_percent = (
        (current_last_traded_price - entry_price) / entry_price * 100
        if entry_price else 0.0)
    if direction == "SHORT":
        profit_loss_percent = -profit_loss_percent

    # .get() with a default everywhere a Redis-stored dict field is read,
    # not direct indexing -- a tracked_position dict could in principle be
    # missing a field (e.g. it was written by an older version of this
    # code before a field was added, or Redis data was manually edited),
    # and a direct tracked_position['field'] access would raise KeyError
    # and crash this exit-alert path instead of degrading gracefully.
    mode_text = tracked_position.get("mode", "UNKNOWN")
    score_at_entry = tracked_position.get("score_at_entry", 0.0)

    return (f"⚠️ EXIT — {symbol} ({mode_text} {direction})\n"
            f"score decayed to {current_score:+.2f} "
            f"(entry {score_at_entry:+.2f})\n"
            f"ltp {current_last_traded_price:.2f} "
            f"(entry {entry_price:.2f}, delta {profit_loss_percent:+.2f}%)\n"
            f"consider closing position")


# ---- Telegram bot state (module-level so the send helpers can reach the running app) ----
TELEGRAM_BOT_STATE = {"application": None, "entry_keyboard_builder": None, "exit_keyboard_builder": None}


async def initialize_telegram_bot(redis_client_async):
    """Sets up the python-telegram-bot Application, registers the button
    callback handler, and sends a startup ping. If TELEGRAM_BOT_TOKEN or
    TELEGRAM_CHAT_ID are missing, or the library import/init fails, the
    bot stays disabled and all alerts fall back to stdout printing."""
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

            # NOTE: edit_message_text() accepts a reply_markup parameter that
            # replaces (or, when None, removes) the message's inline keyboard
            # in the SAME API call. Making one call instead of two (a prior
            # version called edit_message_reply_markup then edit_message_text
            # separately) halves Telegram API usage per button press and
            # avoids a 'Message is not modified' race between the two calls.
            #
            # We read message.text_html (HTML-escaped: '&','<','>' become
            # '&amp;','&lt;','&gt;') and pass parse_mode="HTML" on every
            # edit_message_text call below so Telegram actually interprets
            # those entities instead of showing them as literal text. Using
            # text_html WITHOUT parse_mode="HTML" (an earlier version of
            # this code did exactly that) would show raw HTML-escaped
            # entities to the user, e.g. "S&amp;P" instead of "S&P", the
            # moment any alert text contains such a character. Current
            # alert text has none of those characters today, but this pairs
            # the escaping with the matching parse_mode so it stays correct
            # if that ever changes, rather than leaving a latent bug for
            # whoever edits build_entry_alert_text next.
            original_text_html = callback_query.message.text_html

            if action == "ACK":
                pending_info = await redis_client.get(
                    f"alpha:pending:{callback_query.message.message_id}")
                if pending_info:
                    await redis_client.setex(
                        f"alpha:pos:{symbol}", TRACKED_POSITION_TTL_SECONDS, pending_info)
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
                await redis_client.delete(f"alpha:pos:{symbol}")
                await callback_query.edit_message_text(
                    text=f"{original_text_html}\n\n✔️ DONE — position closed",
                    parse_mode="HTML", reply_markup=None)

            elif action == "HOLD":
                await redis_client.setex(
                    f"alpha:exitcd:{symbol}", EXIT_ALERT_COOLDOWN_SECONDS * 2, "1")
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
    """Gracefully stops the Telegram bot's polling updater and shuts down
    the Application, if one was successfully initialized."""
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
    """Single source of truth for an entry's LONG/SHORT direction. For
    GAP_FADE, the meaningful direction is the gap's own sign (not the
    composite score's sign, which can differ). For all other modes, the
    composite score's sign is the direction. Used both when building the
    entry alert text AND when persisting the pending position, so the
    exit alert can later read the SAME direction back reliably instead
    of re-deriving it from score_at_entry (which is wrong for GAP_FADE,
    since a gap-fade's score and its trade direction are not always the
    same sign)."""
    if mode == "GAP_FADE":
        return "LONG" if score_data.get("gap", 0.0) > 0 else "SHORT"
    return "LONG" if score_data["score"] > 0 else "SHORT"


async def send_entry_alert(redis_client_async, score_data, mode):
    """Sends (or, if the bot is disabled, prints) an entry alert with
    ACK/SKIP buttons, and persists the pending alert details in Redis
    so the ACK handler can look them up later -- even across a restart."""
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


async def send_exit_alert(score_data, tracked_position):
    """Sends (or, if the bot is disabled, prints) an exit alert with
    DONE/HOLD MORE buttons for a previously ACK'd position."""
    alert_text = build_exit_alert_text(
        score_data["symbol"], tracked_position, score_data["score"],
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
    """Checks all suppression conditions in priority order and returns
    (is_suppressed, reason_string). The caller is responsible for caching
    this result for a short interval to avoid hitting Redis/disk on every
    single incoming score message.

    The disk check (os.path.exists) is run via asyncio.to_thread rather
    than called directly, since it is a synchronous/blocking system call.
    On a typical /tmp tmpfs this completes in microseconds, so the
    practical impact is negligible, but calling blocking I/O directly
    inside an asyncio coroutine is still avoided here as good practice."""
    try:
        if bool(await redis_client_async.get("alpha:mute")):
            return True, "muted"
        if await asyncio.to_thread(os.path.exists, MUTE_FILE_PATH):
            return True, "muted"
        if not await redis_client_async.get("alpha:hb:B"):
            return True, "engine_dead"
        if not await redis_client_async.get("alpha:hb:data"):
            return True, "no_data"
    except Exception:
        # Redis itself is unreachable -- fall back to the disk-only mute
        # check so the fail-safe OR semantics still hold.
        mute_file_exists = await asyncio.to_thread(os.path.exists, MUTE_FILE_PATH)
        return mute_file_exists, "muted"
    return False, ""


SCORE_RECEIVE_TIMEOUT_SECONDS = 1.0   # how often the broadcaster loop wakes up even with no data


async def broadcaster_loop():
    """Process C's main loop. Consumes scores published by Process B,
    checks suppression (mute/heartbeats), fires exit alerts for tracked
    positions whose score has decayed, fires entry alerts for newly
    qualifying signals (respecting per-signal cooldowns), and periodically
    prints a summary of counters for observability."""
    zmq_context = zmq.asyncio.Context()
    score_subscriber = zmq_context.socket(zmq.SUB)
    score_subscriber.setsockopt(zmq.RCVHWM, ZMQ_HIGH_WATER_MARK)
    score_subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    score_subscriber.connect(ZMQ_SCORES_ADDRESS)

    # multiprocessing.Process.terminate() sends SIGTERM. Inside an asyncio
    # event loop, the cleanest way to react to that is an
    # asyncio-native signal handler that sets a stop Event, so the main
    # while-loop below can check it and exit through the `finally` block
    # (which shuts down the Telegram bot cleanly) instead of the process
    # being killed mid-flight with pending Telegram/Redis I/O abandoned.
    stop_requested = asyncio.Event()
    running_loop = asyncio.get_running_loop()
    try:
        for signal_number in (signal.SIGTERM, signal.SIGINT):
            running_loop.add_signal_handler(signal_number, stop_requested.set)
    except NotImplementedError:
        # add_signal_handler is unavailable on some platforms (e.g. Windows);
        # the process will still respond to SIGKILL from the launcher's
        # force-kill fallback, just without a clean shutdown path.
        pass

    redis_client_async = redis_asyncio.from_url(
        REDIS_URL, decode_responses=True,
        health_check_interval=30, socket_keepalive=True)

    # Process C's own liveness heartbeat (symmetric with Process A and B).
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
                # A plain, unconditional recv_multipart() blocks forever if
                # Process B stops publishing (market closed, engine down,
                # etc). While blocked, this loop never reaches the periodic
                # stats-print check below, so the console goes silent with
                # no visible indication of "no_data" suppression -- it
                # looks indistinguishable from a crash. Wrapping the recv in
                # asyncio.wait_for() means the loop always wakes up at least
                # once every SCORE_RECEIVE_TIMEOUT_SECONDS even with zero
                # incoming messages, so stats keep printing and the stop
                # signal is checked promptly.
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

                # Refresh the cached suppression check at most once per second.
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
                    # 1) Exit alerts for previously ACK'd positions whose score has
                    # either decayed back toward zero OR reversed hard against the
                    # entry direction. Using abs(current_score) <= threshold alone
                    # is NOT sufficient: if a LONG entry's score reverses sharply
                    # negative (e.g. +8.5 at entry -> -6.0 on a trend reversal),
                    # abs(-6.0) = 6.0 is NOT <= 2.0, so a plain-abs check would
                    # never fire an exit alert even though the position is now
                    # moving strongly AGAINST the user. The check below fires
                    # whenever the score has fallen through the LONG exit
                    # threshold, or risen through the SHORT exit threshold --
                    # covering both "decayed to flat" and "reversed against us".
                    try:
                        tracked_position_json = await redis_client_async.get(
                            f"alpha:pos:{score_data['symbol']}")
                    except Exception:
                        tracked_position_json = None
                        stats_counters["redis_errors"] += 1

                    if tracked_position_json:
                        tracked_position = json.loads(tracked_position_json)
                        position_direction = tracked_position.get(
                            "direction",
                            "LONG" if tracked_position.get("score_at_entry", 0.0) > 0 else "SHORT")
                        position_mode = tracked_position.get("mode", "")

                        # GAP_FADE's direction is derived from the GAP's sign, not
                        # the composite score's sign (see determine_entry_direction) --
                        # the two can legitimately disagree (e.g. a bullish gap-fade
                        # entry can coexist with a bearish composite score from the
                        # other 8 calculators). So for GAP_FADE we must evaluate the
                        # exit condition against gap_fade_raw_score (the same metric
                        # that triggered entry), NOT the composite score. Using the
                        # composite score here was a real bug: it could cause an
                        # exit alert to fire on the very next tick after entry,
                        # because the composite score was never actually inside the
                        # GAP_FADE entry's trigger range to begin with.
                        if position_mode == "GAP_FADE":
                            current_gap_score = score_data.get("gap", 0.0)
                            if position_direction == "LONG":
                                should_exit = current_gap_score <= EXIT_ALERT_SCORE_THRESHOLD
                            else:
                                should_exit = current_gap_score >= -EXIT_ALERT_SCORE_THRESHOLD
                        else:
                            current_score = score_data["score"]
                            if position_direction == "LONG":
                                should_exit = current_score <= EXIT_ALERT_SCORE_THRESHOLD
                            else:
                                should_exit = current_score >= -EXIT_ALERT_SCORE_THRESHOLD

                        if should_exit:
                            try:
                                if await is_exit_cooldown_expired(
                                        redis_client_async, score_data["symbol"]):
                                    await send_exit_alert(score_data, tracked_position)
                                    stats_counters["exit_alerts_sent"] += 1
                            except Exception as exit_error:
                                stats_counters["redis_errors"] += 1
                                print(f"[C] exit err: {exit_error!r}", flush=True)

                    # 2) Entry alerts for newly qualifying signals.
                    # No global rate limiting -- this is an advisory system,
                    # the user decides which alerts to act on. Only a
                    # per-(symbol, mode) cooldown is applied, to avoid
                    # sending a duplicate of the SAME signal repeatedly.
                    signal_mode = determine_signal_mode(
                        score_data["score"], score_data["relative_volume"],
                        score_data.get("gap", 0.0))
                    if not signal_mode:
                        stats_counters["no_qualifying_mode"] += 1
                    else:
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
                await asyncio.sleep(0.1)   # avoid a tight error loop
    finally:
        await shutdown_telegram_bot()
        # Explicitly close the async Redis connection pool and terminate
        # the ZMQ context. Without this, redis.asyncio leaves its
        # connection pool's underlying sockets open until Python's
        # garbage collector eventually finalizes the client (which for a
        # process that's exiting right after this point may never happen
        # cleanly), producing a "Unclosed client session" / "Unclosed
        # connector" warning on stderr. zmq_context.term() blocks until
        # the context's I/O threads have shut down, mirroring the same
        # cleanup already done in alpha_engine()'s shutdown path.
        try:
            await redis_client_async.aclose()
        except Exception as redis_close_error:
            print(f"[C] redis close err: {redis_close_error!r}", flush=True)
        try:
            zmq_context.term()
        except Exception as zmq_close_error:
            print(f"[C] zmq context term err: {zmq_close_error!r}", flush=True)


def run_broadcaster_process():
    """Entry point wrapper so Process C can run the async broadcaster_loop
    inside multiprocessing.Process (which expects a plain sync callable)."""
    asyncio.run(broadcaster_loop())


# ============================================================
# LAUNCHER: self-healing (auto-respawn) + graceful shutdown
# ============================================================
# Derive the raw filesystem paths from the ipc:// URIs so the cleanup
# logic below doesn't have to hardcode them a second time.
IPC_SOCKET_FILE_PATHS = [
    address[len("ipc://"):]
    for address in (ZMQ_TICKS_ADDRESS, ZMQ_SCORES_ADDRESS)
    if address.startswith("ipc://")
]
MAX_RESTARTS_ALLOWED_PER_WINDOW = 6      # crash-loop breaker
RESTART_COUNTING_WINDOW_SECONDS = 3600


def cleanup_ipc_socket_files():
    """Removes stale ZMQ ipc:// socket files. This is required because
    UNIX domain sockets are files on disk that are NOT automatically
    removed by the kernel when a process dies without closing them
    cleanly (e.g. via SIGKILL) -- the next bind() attempt on the same
    path would otherwise fail with 'Address already in use'."""
    for socket_path in IPC_SOCKET_FILE_PATHS:
        try:
            if os.path.exists(socket_path):
                os.remove(socket_path)
                print(f"[launcher] removed stale ipc socket: {socket_path}", flush=True)
        except Exception as cleanup_error:
            print(f"[launcher] ipc cleanup warn ({socket_path}): {cleanup_error!r}", flush=True)


def spawn_all_processes():
    """Creates and starts fresh Process objects for A, B, and C, and
    returns the list of process handles."""
    process_list = [
        Process(target=data_factory,           name="A-DataFactory"),
        Process(target=alpha_engine,           name="B-AlphaEngine"),
        Process(target=run_broadcaster_process, name="C-Broadcaster"),
    ]
    for process in process_list:
        process.start()
    return process_list


def shutdown_all_processes(process_list, shutdown_reason):
    """Gracefully terminates every process in process_list: sends
    SIGTERM, waits up to 5 seconds total for all of them to exit, then
    force-kills (SIGKILL) any stragglers. Finally cleans up any leftover
    ZMQ ipc socket files."""
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
    cleanup_ipc_socket_files()   # defensive: clear sockets left behind by a prior ungraceful exit
    running_processes = spawn_all_processes()

    stop_event = threading.Event()
    for signal_number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signal_number, lambda *_args: stop_event.set())

    recent_restart_timestamps = deque()   # crash-loop breaker: timestamps of recent restarts

    try:
        while not stop_event.is_set():
            dead_processes = [process for process in running_processes if not process.is_alive()]

            if dead_processes:
                exit_codes_by_process_name = {
                    process.name: process.exitcode for process in dead_processes}

                # EXIT_CODE_FATAL_CONFIG means a process hit a non-retryable
                # error (missing/invalid .env credentials, bad TOTP secret,
                # Angel login rejected, etc). Respawning would just repeat
                # the exact same failure until the crash-loop breaker trips
                # anyway, burning ~2 minutes and cluttering the log with
                # identical error messages for a problem that only a human
                # editing .env can fix. Stop immediately instead.
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

                # Exit code 2 is our own signal for "planned restart" (e.g. the
                # scheduled JWT-refresh self-exit in data_factory).
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
                time.sleep(2)   # brief pause to avoid a tight crash loop
                running_processes = spawn_all_processes()
                continue

            time.sleep(1)

        shutdown_all_processes(running_processes, "signal received")
    except KeyboardInterrupt:
        shutdown_all_processes(running_processes, "KeyboardInterrupt")
