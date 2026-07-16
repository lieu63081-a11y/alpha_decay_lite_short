import os
import time
import math
import json
import signal
import threading
from collections import deque
from dataclasses import dataclass, field

import zmq
import redis
from dotenv import load_dotenv

load_dotenv()

ZMQ_TICKS_ADDRESS   = "ipc:///tmp/alpha_ticks.ipc"
ZMQ_SCORES_ADDRESS  = "ipc:///tmp/alpha_scores.ipc"
REDIS_URL           = os.getenv("REDIS_URL", "redis://localhost:6379/0")

SPREAD_RATIO_MAX             = 0.0015
EMA_TIME_CONSTANT_SECONDS    = 1.0

ZMQ_HIGH_WATER_MARK          = 10000
LATENCY_WARNING_MS           = 100

RVOL_WARMUP_SECONDS          = 300
RESET_DROP_FRACTION_THRESHOLD = 0.5
PEAK_SCORE_HALF_LIFE_SECONDS  = 60.0

PROCESS_HEARTBEAT_TTL_SECONDS      = 3
DATA_FLOW_HEARTBEAT_TTL_SECONDS    = 5
HEARTBEAT_WRITE_INTERVAL_SECONDS   = 1

def clip_value(value, minimum, maximum):
    return max(minimum, min(maximum, value))

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

