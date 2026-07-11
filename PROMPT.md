# Alpha-Decay Lite — Full Rebuild Prompt (v2, hardened)

Signal-only NSE cash equity alerting engine. No F&O, no derivatives, no
automated trading. User executes manually on their broker app. This
document is a complete, self-contained spec: every calculator formula,
every edge case, and every design decision below was arrived at through
multiple rounds of adversarial code review + simulation-based verification
against a working implementation. Treat every "MUST" below as load-bearing
— it exists because a specific, verified bug or failure mode was found and
fixed at that exact point. Do not "simplify" these away without re-deriving
the failure mode first.

---

## 1. Purpose & Scope

Angel One SmartAPI से market data लो, per-tick score compute करो, high-quality
Buy/Sell alerts Telegram पर भेजो. User manually order place करे broker app पर.

- **Universe:** NSE Cash Equity only. No F&O. Configurable symbol list via
  env var (comma-separated, e.g. `RELIANCE,TCS,HDFCBANK`); default target is
  NIFTY 50 + liquid EQ segment stocks (daily turnover ≥ ₹100 Cr).
- **Exclude (operator responsibility, not enforced in code):** circuit
  stocks, T2T, BE/T-group/ST/GSM restricted, spread > 0.15% (this one IS
  enforced in code — see Calculator #6), pending-news-within-24h.
- **Direction:** Bullish → LONG suggestion (CNC or MIS, user's choice).
  Bearish → SHORT suggestion (MIS intraday only, user covers manually by
  3:15 PM — not enforced by the system, advisory only).
- **Design contract:** यह system कभी order fire नहीं करता। सिर्फ़ actionable
  signals Telegram पर deliver करता है। सारा execution risk और final trade
  decision user के पास है। Algo advisory है, algo execution नहीं।

---

## 2. Architecture (3 processes, self-healing launcher)

```
Angel WS (mode 3)  →  data_factory()   ─ZMQ ticks─→   alpha_engine()   ─ZMQ scores─→  broadcaster_loop()  →  Telegram
                       (Process A)                     (Process B)                    (Process C)
```

- **Process A — Data Factory:** Angel `SmartWebSocketV2` mode 3 (snap-quote)
  ingest, ZMQ `PUB` over `ipc://` publish of raw ticks.
- **Process B — Alpha Engine:** 9 calculators, time-weighted EMA smoother,
  composite feature score in [-10, +10], ZMQ `SUB` (ticks) + `PUB` (scores).
- **Process C — Signal Broadcaster:** format alert, per-(symbol,mode)
  cooldown/dedup, deliver to Telegram bot with inline buttons.
- **Redis** for all cross-process state (heartbeats, mute flags, cooldowns,
  tracked positions). **No TimescaleDB in the base build** (tick_store is a
  separate, standalone service — see §11).
- Every process gets its **own fresh `zmq.Context()`** (never shared/forked)
  to avoid inherited socket state across process boundaries.
- A single top-level launcher (`if __name__ == "__main__":`) spawns all
  three, monitors `process.is_alive()`, and respawns **all three together**
  on any single process death — with a crash-loop breaker (see §9).

### Naming convention (non-negotiable)
Every variable and function name MUST be full and descriptive — no
single-letter or cryptic abbreviations anywhere in the codebase (main file,
any helper module, README). E.g. `symbol_state` not `st`, `last_traded_price`
not `ltp`, `calculate_relative_volume` not `_c2_rvol`. This was an explicit,
repeated requirement — apply it to every file, not just the main script.

---

## 3. Startup / Timezone (MUST)

- Force `os.environ["TZ"] = "Asia/Kolkata"` then call `time.tzset()` before
  any other import that touches time — needed because `calculate_gap_fade`
  and `calculate_session_weight` do IST-hour/minute comparisons via
  `time.localtime()`.
- `time.tzset()` is **Unix-only** (Python docs: "Availability: Unix"). On
  Windows, setting `TZ` alone does nothing — `time.localtime()` would
  silently use the wrong timezone with no error, corrupting every
  session-window calculation without any visible symptom. **MUST** fail
  fast: `if hasattr(time, "tzset"): time.tzset() else: print fatal + exit(1)`.
  This deployment targets Linux VPS only — this is a guard against running
  on the wrong platform by mistake, not a cross-platform feature request.

---

## 4. Configuration Constants (exact values, with rationale)

```python
SPREAD_RATIO_MAX                 = 0.0015   # 0.15%
EMA_TIME_CONSTANT_SECONDS        = 1.0      # tau for calculator #9
COOLDOWN_SECONDS_BY_MODE = {"MOMENTUM": 300, "MEAN_REVERSION": 45, "GAP_FADE": 180}
ZMQ_HIGH_WATER_MARK               = 10000
LATENCY_WARNING_MS                = 100
MUTE_STATUS_CACHE_SECONDS         = 1.0
RVOL_WARMUP_SECONDS               = 300     # 5 minutes
RESET_DROP_FRACTION_THRESHOLD     = 0.5     # >50% cumulative-volume drop = broker reset, not stale packet
PEAK_SCORE_HALF_LIFE_SECONDS      = 60.0
AUTO_RESTART_AFTER_HOURS          = 20      # < Angel JWT ~24-28h expiry, configurable via env
TICK_STARVATION_WATCHDOG_SECONDS  = 90      # see §6 watchdog
EXIT_CODE_PLANNED_RESTART         = 2       # launcher: respawn immediately, no crash-loop penalty
EXIT_CODE_FATAL_CONFIG            = 3       # launcher: do NOT retry, exit(1) immediately
PROCESS_HEARTBEAT_TTL_SECONDS     = 3
DATA_FLOW_HEARTBEAT_TTL_SECONDS   = 5
HEARTBEAT_WRITE_INTERVAL_SECONDS  = 1
EXIT_ALERT_SCORE_THRESHOLD        = 2.0
EXIT_ALERT_COOLDOWN_SECONDS       = 300
TRACKED_POSITION_TTL_SECONDS      = 6 * 3600   # auto-cleanup stale positions after 6h+
PENDING_ALERT_TTL_SECONDS         = 24 * 3600
MAX_RESTARTS_ALLOWED_PER_WINDOW   = 6
RESTART_COUNTING_WINDOW_SECONDS   = 3600
```

**`TRADING_UNIVERSE` parsing (MUST strip + filter empty):**
```python
TRADING_UNIVERSE = [s.strip() for s in os.getenv("UNIVERSE", "RELIANCE,TCS,HDFCBANK").split(",") if s.strip()]
```
A plain `.split(",")` with no `.strip()` silently produces symbols like
`" TCS"` if the user's `.env` has spaces after commas (a very natural way
to type it) — Angel's scrip master has no entry for `" TCS"`, so that
symbol gets silently dropped from the universe with only a log warning, no
crash. The `if s.strip()` also guards a trailing comma producing a stray
empty entry.

---

## 5. Rolling Window Infrastructure (amortized O(1), out-of-order-safe)

```python
class RollingWindowSum:
    def __init__(self, window_seconds):
        self.window_seconds = window_seconds
        self.entries = deque()       # (timestamp, value), MUST stay chronological
        self.running_sum = 0.0

    def add(self, timestamp, value):
        # MUST reject (not append) any sample older than the current back
        # of the deque. Every consumer assumes strictly non-decreasing
        # timestamps: eviction below only inspects the FRONT of the deque,
        # and span_seconds() assumes the back is always the newest. An
        # out-of-order sample (network jitter, WS reconnect replay,
        # duplicate/retransmitted tick) appended anyway breaks BOTH
        # assumptions: span_seconds() can go NEGATIVE (misfires the RVOL
        # warmup check for that one tick), and the sample's VALUE gets
        # added to running_sum and sits unevicted for up to the FULL
        # window duration since front-only eviction can never reach an
        # entry buried behind newer ones by arrival order — verified via
        # simulation: an anomalous value injected out of order inflated a
        # 20-minute window's running_sum for the entire ~20 minutes before
        # eviction caught up.
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
```

**MUST call `.add()` on EVERY tick, even with `value=0.0`.** Eviction only
runs inside `add()`. If you skip calling it during a quiet stretch (no
aggressive trades, say), stale data from minutes ago keeps being read as
"the last 30 seconds" because nothing ever triggered eviction.

The identical out-of-order-rejection pattern MUST also be applied to the
plain `deque`-based `last_traded_price_history_30s` (used by Calculator
#4), since it is not built on `RollingWindowSum`:
```python
if not price_history or current_timestamp >= price_history[-1][0]:
    price_history.append((current_timestamp, last_traded_price))
    cutoff = current_timestamp - 30
    while price_history and price_history[0][0] < cutoff:
        price_history.popleft()
```

---

## 6. Process A — Data Factory

### 6.1 Scrip master download (MUST retry, bounded)
```python
def download_scrip_master_with_retry():
    max_attempts = 4
    retry_delay_seconds = 3
    for attempt in range(1, max_attempts + 1):
        try:
            return requests.get(SCRIP_MASTER_URL, timeout=60).json()
        except (requests.exceptions.RequestException, ValueError) as e:
            if attempt == max_attempts:
                raise
            time.sleep(retry_delay_seconds)
            retry_delay_seconds *= 2
```
A bare `requests.get()` with zero exception handling crashes Process A on
ANY transient DNS/network hiccup at startup — indistinguishable from any
other crash to the launcher, so repeated transient failures can burn
through the crash-loop budget and trip the launcher's FATAL shutdown
before the network even recovers. Retry is BOUNDED — a persistent outage
still raises after 4 attempts and falls back to the launcher's crash-loop
breaker as designed, not an infinite hang.

### 6.2 Tick field parsing (MUST null-safe every divided field, including nested)
Angel's feed can send an explicit JSON `null` for any field (parsed as
Python `None`). `dict.get(key, default)` ONLY returns `default` when the
key is **absent** — if the key is present with value `None`, `.get()`
returns `None`, and `None / 100.0` raises `TypeError`, crashing the
callback (which runs on the SDK's own thread). **MUST** wrap every field
divided by 100 with `(value or 0)`:
```python
"last_traded_price": (market_data_message.get("last_traded_price") or 0) / 100.0,
"daily_vwap": (market_data_message.get("average_traded_price") or 0) / 100.0,
"open_price": (market_data_message.get("open_price_of_the_day") or 0) / 100.0,
"previous_close_price": (market_data_message.get("closed_price") or 0) / 100.0,
```
This same null-safety **MUST also apply to nested depth-level dicts**
(`best_5_buy_data[0]["price"]`), which is a separate, easy-to-miss spot:
```python
"best_bid_price": ((best_5_buy_levels[0].get("price") or 0) / 100.0) if best_5_buy_levels else 0.0,
"best_ask_price": ((best_5_sell_levels[0].get("price") or 0) / 100.0) if best_5_sell_levels else 0.0,
```
`.get("price", 0)` alone is NOT sufficient here — it only helps when the
key is missing, not when it's explicitly `null`. This exact nested case
was found AFTER the top-level fields were already fixed in an earlier
round — don't repeat that gap.

Also support an `ANGEL_BID_ASK_IS_SWAPPED` env flag to swap
`best_5_buy_data`/`best_5_sell_data` if a diagnostic script confirms the
SDK version reports them backwards (see §11, `diag_bidask.py`).

### 6.3 Graceful shutdown (MUST NOT poll after a blocking call)
`websocket_client.connect()` is **BLOCKING** — it occupies this process's
only thread until `close_connection()` is called or the SDK gives up. A
signal handler CANNOT set a flag for a `while` loop to check, because there
is no such loop below `connect()` — it must actively call
`websocket_client.close_connection()` to unblock `connect()`, after which
cleanup runs normally:
```python
def handle_stop_signal(_signum, _frame):
    websocket_client.close_connection()
signal.signal(signal.SIGTERM, handle_stop_signal)
signal.signal(signal.SIGINT, handle_stop_signal)
websocket_client.connect()   # blocks here
# cleanup after connect() returns: tick_publisher.close(); zmq_context.term()
```
Do NOT implement this as `while not stop_requested.is_set(): time.sleep(1)`
placed after `connect()` — that loop would never execute since `connect()`
never returns control back to this thread on its own.

### 6.4 Tick-starvation watchdog (MUST be market-hours-gated)
A "zombie" half-open TCP connection (network drops the link without either
side sending a TCP FIN/RST) leaves `connect()` never returning — the
process stays fully "alive" to the OS forever, invisible to the launcher's
`process.is_alive()`-based respawn logic. A background thread MUST force-
exit if no tick has arrived for `TICK_STARVATION_WATCHDOG_SECONDS` (90s):
```python
def is_within_trading_hours(timestamp):
    local_time = time.localtime(timestamp)
    if local_time.tm_wday >= 5:  # Sat/Sun
        return False
    hour, minute = local_time.tm_hour, local_time.tm_min
    after_open = (hour > 9) or (hour == 9 and minute >= 15)
    before_close = (hour < 15) or (hour == 15 and minute <= 30)
    return after_open and before_close

def watch_for_tick_starvation():
    while True:
        time.sleep(15)
        if not is_within_trading_hours(time.time()):
            continue   # CRITICAL: do not check tick staleness outside market hours
        if (last_tick_received_at["timestamp"] > 0
                and time.time() - last_tick_received_at["timestamp"] > TICK_STARVATION_WATCHDOG_SECONDS):
            os._exit(EXIT_CODE_PLANNED_RESTART)
```
**The market-hours gate is load-bearing, not optional.** Without it, this
watchdog misfires every single night and weekend (when zero ticks for
hours is completely normal), forcing repeated pointless restarts and very
likely tripping the launcher's crash-loop breaker — which shuts down the
ENTIRE system (Process B and C too), a far worse outcome than the
zombie-connection bug this exists to fix. This function deliberately does
NOT know about exchange holidays (a static holiday table needs yearly
upkeep; treating a holiday as "market closed but watchdog stays quiet
anyway" costs nothing since no ticks arrive on a holiday regardless).

### 6.5 Heartbeats
- `alpha:hb:A` (process-alive, `PROCESS_HEARTBEAT_TTL_SECONDS`=3s TTL) via
  a shared `start_heartbeat_thread` helper (also used by B and C).
- `alpha:hb:data` (data-is-flowing, `DATA_FLOW_HEARTBEAT_TTL_SECONDS`=5s TTL)
  — **MUST be refreshed from TWO places**: (a) the same background
  heartbeat thread's periodic check, AND (b) **directly inside the tick
  callback itself**, throttled to once per second. Relying on (a) alone
  creates a race: the background thread's own schedule is independent of
  tick arrival, so if the market goes quiet for >5s and then a fresh tick
  arrives, the Redis key can still show "expired" for up to ~1 more second
  until the thread's next scheduled wake — incorrectly suppressing a
  legitimate signal as `no_data` right when data has already resumed.

### 6.6 JWT auto-refresh
Schedule a `threading.Timer(AUTO_RESTART_AFTER_HOURS * 3600, ...)` that
calls `os._exit(EXIT_CODE_PLANNED_RESTART)` — NOT a graceful
`sys.exit()`/return, because there is no orderly code path to unwind
through while `connect()` is blocking the only thread. The launcher's
respawn-on-death IS the recovery mechanism, exactly as it already is for
the watchdog case above.

### 6.7 Fatal config errors (MUST distinguish from retryable crashes)
Missing/invalid `.env` credentials, invalid TOTP secret, or a rejected
Angel login MUST call `os._exit(EXIT_CODE_FATAL_CONFIG)`, NOT a generic
`sys.exit(1)` or an unhandled exception. The launcher special-cases this
exit code to stop immediately without retrying — these errors will not
fix themselves without a human editing `.env`, so retrying just burns the
crash-loop budget on an identical failure 6 times before giving up anyway.

---

## 7. Process B — Alpha Engine (9 Calculators)

`SymbolState` per symbol holds: `volume_sum_last_60_seconds` /
`volume_sum_last_20_minutes` (both `RollingWindowSum`),
`aggressive_signed_volume_30s` / `aggressive_absolute_volume_30s` (both
`RollingWindowSum`, window=30s), `last_traded_price_history_30s` (plain
deque), `last_cumulative_volume` (int, **-1 = uninitialized**),
`last_traded_price`, `smoothed_ema_score`, `last_tick_timestamp`,
`peak_score_amplitude`.

### #1 — `calculate_vwap_distance`
```python
percent_distance = (last_traded_price - daily_vwap) / daily_vwap * 100
return clip(percent_distance * 5.0, -2.0, 2.0)
```
Guard: return 0.0 if `daily_vwap` or `last_traded_price` is falsy.

### #2 — `calculate_relative_volume` (RVOL)
```python
if window.span_seconds() < RVOL_WARMUP_SECONDS:
    return 1.0   # neutral during warmup
baseline_volume_sum = max(window.running_sum - volume_sum_last_60_seconds.running_sum, 0.0)
elapsed_baseline_minutes = max(1.0, (window.span_seconds() - 60) / 60)
baseline_minutes = min(19.0, elapsed_baseline_minutes)   # MUST use elapsed time, NOT a hardcoded 19.0
baseline_volume_per_minute = baseline_volume_sum / baseline_minutes
return volume_sum_last_60_seconds.running_sum / max(baseline_volume_per_minute, 1.0)
```
**MUST NOT hardcode the divisor as `19.0`.** Dividing by a fixed 19.0 while
the window has only been accumulating for, say, 5-6 minutes understates
the baseline by ~3-4x, inflating RVOL by the same factor — verified: at 5
minutes of perfectly steady (non-spiking) volume, the fixed-divisor version
reported RVOL=4.75 where the true value is 1.00, which can spuriously
satisfy MOMENTUM's `rvol >= 1.5` gate on ordinary volume. The most recent
60 seconds is explicitly excluded from the baseline to avoid
self-inclusion bias (the current spike shouldn't inflate its own baseline).

### #3 — `calculate_aggressive_buy_pressure`
```python
gross = aggressive_absolute_volume_30s.running_sum
if gross < 100: return 0.0
net = aggressive_signed_volume_30s.running_sum
return clip(net / gross * 2.0, -2.0, 2.0)
```
Uses a **rolling 30-second window**, not a single tick's classification —
a single-tick signal is far too noisy on its own.

### #4 — `calculate_absorption` (symmetric, trade-classified)
```python
if len(price_history) < 5 or not last_traded_price: return 0.0
price_range_ratio = (max_price_in_window - min_price_in_window) / last_traded_price
if price_range_ratio > 0.001: return 0.0   # price not flat enough
if aggressive_signed_volume_30s.span_seconds() < 10: return 0.0   # not enough history
gross = aggressive_absolute_volume_30s.running_sum
if gross < 100: return 0.0
net_to_gross_ratio = aggressive_signed_volume_30s.running_sum / gross
if net_to_gross_ratio < -0.4: return  1.5   # heavy net SELL absorbed, flat price -> BULLISH
if net_to_gross_ratio >  0.4: return -1.5   # heavy net BUY absorbed, flat price -> BEARISH trap
return 0.0
```

### #5 — `calculate_book_imbalance`
```python
combined = total_buy_quantity + total_sell_quantity
if not combined: return 0.0
return clip((total_buy_quantity - total_sell_quantity) / combined * 2, -2.0, 2.0)
```

### #6 — `calculate_spread_ratio` (filter, gates composite score to 0)
```python
if last_traded_price and best_bid_price and best_ask_price:
    return (best_ask_price - best_bid_price) / last_traded_price
return 1.0   # missing quote data -> treat as "spread too wide" (fail SAFE, not fail open)
```
In `compute_feature_score`: `if spread_ratio > SPREAD_RATIO_MAX: return (0.0, relative_volume, 0.0, gap_fade_raw_score_unrestricted)` — the score is gated to zero but `relative_volume` and the unrestricted gap metric are still returned (independently useful / needed by the exit path, see #7 below).

### #7 — Gap-Fade: TWO functions, not one (MUST split entry-gate from exit-metric)
```python
def calculate_gap_fade_unrestricted(tick_data):
    # the core formula, NO time gating
    if not previous_close_price or not open_price:
        return 0.0   # MUST check open_price too, not just previous_close_price --
                      # otherwise (0 - previous_close)/previous_close produces an
                      # artificial -100% "gap" before the exchange has published
                      # an official open, clipping to +3.0 with no real gap
    gap_percent = (open_price - previous_close_price) / previous_close_price * 100
    return clip(-gap_percent, -3.0, 3.0)

def calculate_gap_fade(tick_data):
    # ENTRY-only gate: 9:15:00-9:29:59 IST
    local_time = time.localtime(tick_data["timestamp"])
    if not (local_time.tm_hour == 9 and 15 <= local_time.tm_min < 30):
        return 0.0
    return calculate_gap_fade_unrestricted(tick_data)
```
**Why two functions:** the ENTRY-gated version feeds the composite score
and `determine_signal_modes`' GAP_FADE qualification (should only fire NEW
entries inside the window). The UNRESTRICTED version is published
separately (`"gap_unrestricted"` field) and MUST be used by Process C's
exit check for an already-tracked GAP_FADE position instead of the gated
value. **Verified bug this fixes:** using the gated value for exits meant
EVERY open GAP_FADE position got force-exited at exactly 9:30:00 daily —
the moment the window ends, the gated function unconditionally returns
0.0, which is always `<= EXIT_ALERT_SCORE_THRESHOLD` — regardless of
whether the real-world gap had actually closed at all (simulated: entry on
a genuine 2.5% gap, price completely unchanged, still produced a false
exit the instant the clock passed 9:30:00).

### #8 — `calculate_session_weight`
```python
if hour == 9 and 15 <= minute < 45: return 1.2   # OPEN_VOL — MUST start at :15, not :00
                                                   # (09:00-09:14 is NSE's pre-open session,
                                                   #  not continuous trading; including it gave
                                                   #  pre-open ticks the same 1.2x boost as
                                                   #  genuine post-open ticks)
if (hour == 14 and minute >= 45) or (hour == 15 and minute <= 30): return 1.1   # CLOSE_HOUR
return 0.8   # MID_QUIET
```

### #9 — EMA Smoother + Peak Amplitude (inline in the main tick loop, not a standalone function)
```python
if symbol_state.last_tick_timestamp == 0:
    # First tick ever: EMA has no choice but smoothed = raw (no prior state).
    symbol_state.smoothed_ema_score = raw_composite_score
    # BUT peak MUST seed at 0.0, NOT abs(raw_composite_score). Market-open
    # ticks are frequently anomalous (illiquid opening quote, stale first
    # snap-quote) and peak is broadcast directly in every Telegram alert
    # as if it reflects genuinely sustained momentum. Seeding from a
    # single unsmoothed outlier let one bad opening tick (e.g. spurious
    # +10.0) broadcast a misleadingly high peak for well over a minute
    # after the smoothed score had already normalized — verified via
    # simulation: outlier first tick of 10.0 then steady genuine ticks of
    # 1.0 kept peak >= 5.0 for a full 60s under seed-from-raw, vs peak
    # <= 2.2 by 60s when seeded at 0.0.
    symbol_state.peak_score_amplitude = 0.0
elif tick_data["timestamp"] > symbol_state.last_tick_timestamp:
    delta_time_seconds = tick_data["timestamp"] - symbol_state.last_tick_timestamp
    ema_alpha = 1.0 - math.exp(-delta_time_seconds / EMA_TIME_CONSTANT_SECONDS)
    symbol_state.smoothed_ema_score = ema_alpha * raw_composite_score + (1 - ema_alpha) * symbol_state.smoothed_ema_score
    peak_decay_factor = math.exp(-delta_time_seconds * math.log(2) / PEAK_SCORE_HALF_LIFE_SECONDS)
    symbol_state.peak_score_amplitude = max(symbol_state.peak_score_amplitude * peak_decay_factor, abs(symbol_state.smoothed_ema_score))
# else: same-timestamp batched/duplicate tick -> preserve previous smoothed score & peak unchanged

# CRITICAL: last_tick_timestamp MUST only ever move FORWARD.
symbol_state.last_tick_timestamp = max(symbol_state.last_tick_timestamp, tick_data["timestamp"])
```
The `max()` guard on the last line is not optional: an out-of-order tick
with an earlier timestamp, if allowed to regress `last_tick_timestamp`,
causes the NEXT in-order tick's `delta_time_seconds` to be overstated,
distorting both the EMA alpha and the peak decay factor (verified via
trace: tick1 ts=5, out-of-order tick2 ts=3 (correctly skipped for the EMA
update itself), tick3 ts=6 — without the `max()` guard,
`last_tick_timestamp` would incorrectly be left at 3 after tick2, making
tick3 compute `delta_time_seconds = 6-3 = 3` instead of the correct `6-5 = 1`).

### Combination formula
```python
if aggressive_buy_score * absorption_score >= 0:   # same sign -> dampen redundancy
    combined_flow_score = max(aggressive_buy_score, absorption_score) + 0.4 * min(aggressive_buy_score, absorption_score)
else:                                                # opposite signs -> additive net pressure
    combined_flow_score = aggressive_buy_score + absorption_score

composite_score = clip(
    session_weight * (2.0 * vwap_distance_score + 1.5 * combined_flow_score
                       + 1.5 * book_imbalance_score + 1.0 * gap_fade_raw_score),
    -10.0, 10.0)
```
`compute_feature_score` returns **4 values**: `(composite_score,
relative_volume, gap_fade_raw_score, gap_fade_raw_score_unrestricted)`.

### Cumulative-volume watermark (MUST distinguish stale-packet vs. genuine reset)
```python
if symbol_state.last_cumulative_volume < 0:
    symbol_state.last_cumulative_volume = tick_data["cumulative_day_volume"]
    delta_volume = 0   # first tick ever, don't treat the whole day's volume as one spike
elif tick_data["cumulative_day_volume"] >= symbol_state.last_cumulative_volume:
    delta_volume = tick_data["cumulative_day_volume"] - symbol_state.last_cumulative_volume
    symbol_state.last_cumulative_volume = tick_data["cumulative_day_volume"]
else:
    drop_amount = symbol_state.last_cumulative_volume - tick_data["cumulative_day_volume"]
    looks_like_a_reset = (symbol_state.last_cumulative_volume > 0
                          and drop_amount > symbol_state.last_cumulative_volume * RESET_DROP_FRACTION_THRESHOLD)
    if looks_like_a_reset:
        # Clamp to >= 0: a malformed/negative cumulative_day_volume rebasing
        # the watermark to a negative value would make the NEXT tick's
        # `< 0` check above misfire as "first tick ever" again.
        symbol_state.last_cumulative_volume = max(tick_data["cumulative_day_volume"], 0)
    delta_volume = 0
```
Two DIFFERENT real causes produce a lower-than-watermark reading, and they
need OPPOSITE handling:
- **(a) Out-of-order/delayed packet** (small drop, bounded by a few
  seconds of plausible trading volume): MUST reject — `delta_volume = 0`,
  watermark unchanged. Applying the update anyway corrupts the watermark
  downward and double-counts volume on the next in-order tick.
- **(b) Genuine broker-side counter reset** (a large drop — some broker
  backends occasionally reset a symbol's cumulative counter mid-day):
  MUST rebase — `delta_volume = 0` for this tick, watermark set to the
  new (clamped-non-negative) value. If you handle this the same as (a)
  (pure reject), a genuine reset permanently mutes the symbol's
  volume-derived signals for the rest of the day, since the counter would
  need to climb back past its old high-water mark before any tick could
  pass the `>=` check again.
- Distinguish via `RESET_DROP_FRACTION_THRESHOLD` (0.5): a drop of more
  than 50% of the current watermark is treated as case (b); anything
  smaller is case (a).

---

## 8. Process C — Broadcaster + Telegram Bot

### `determine_signal_modes` — MUST return a LIST, not a single mode
```python
def determine_signal_modes(composite_score, relative_volume, gap_fade_raw_score=0.0):
    qualifying_modes = []
    if abs(gap_fade_raw_score) >= 2.0:
        qualifying_modes.append("GAP_FADE")
    absolute_score = abs(composite_score)
    if absolute_score >= 8 and relative_volume >= 1.5:
        qualifying_modes.append("MOMENTUM")
    elif absolute_score >= 3 and relative_volume < 2.0:   # NO upper bound on absolute_score here
        qualifying_modes.append("MEAN_REVERSION")
    return qualifying_modes
```
Two separate, previously-real bugs this exact shape fixes:
1. **GAP_FADE must NOT take priority and short-circuit.** An earlier
   version checked GAP_FADE first and returned immediately on a match —
   a modest gap (e.g. 2.1, barely over its own 2.0 threshold) completely
   discarded a simultaneously-occurring, much stronger MOMENTUM signal
   (verified: score=9.5, rvol=5.0, gap=2.1 returned only `["GAP_FADE"]`).
   GAP_FADE is computed independently of the composite score, so it can
   legitimately co-occur with either of the other two modes — return
   every qualifying mode, not just the first match.
2. **No upper bound on MEAN_REVERSION's score check ("dead zone" fix).**
   A version with `elif 3 <= absolute_score < 8 and relative_volume < 2.0`
   created a dead zone: a score of 9.5 (above MOMENTUM's threshold) with
   relative_volume=1.2 (below MOMENTUM's own volume gate) matched
   NEITHER branch — MOMENTUM failed on volume, MEAN_REVERSION failed on
   its `< 8` upper bound — and the signal was silently dropped entirely
   despite an unusually strong price move (verified: returned `[]`).
   MOMENTUM and MEAN_REVERSION remain mutually exclusive via the `elif`
   itself (gated on MOMENTUM's condition not having matched), just
   without an artificial extra score-based restriction on the fallback.

### Kill switches / suppression (5 layers)
1. **WS data flow** — `alpha:hb:data` (5s TTL) → `no_data` suppression.
2. **Alpha Engine alive** — `alpha:hb:B` (3s TTL) → `engine_dead` suppression.
3. **Manual mute** — Redis `alpha:mute` **OR** disk `MUTE_FILE` (fail-safe
   OR: either alone is enough to mute). Compare the Redis value with
   **exact string equality `== "1"`**, never `bool(value)` — Redis always
   returns strings, and `bool("0")` is `True` in Python, which would treat
   an explicit "not muted" flag of `"0"` as muted (the opposite of
   intended). **Redis-down MUST also count as a suppression condition**
   (`redis_down` reason) — the earlier bug here returned
   `(mute_file_exists, "muted")` on a Redis exception, i.e. `(False,
   "muted")` when no disk flag existed, meaning "NOT suppressed" at
   exactly the moment none of layers #1/#2 could even be checked (they
   also depend on Redis). A kill switch must fail SAFE (suppress) on its
   own dependency failing, not fail open.
4. **Spread filter** — per-symbol, gates composite score to 0 (see
   Calculator #6).
5. **Per-(symbol, mode) cooldown** — Redis `SET NX EX`, prevents the SAME
   signal from re-firing every tick while the score stays inside its band.

**No global rate limit, by design.** This is explicitly rejected — the
system is advisory-only, the user decides which alerts to act on, so
every qualifying signal (subject only to cooldowns, not a global cap) is
delivered. Do not add a "max N alerts/hour" limiter.

### Position tracking — MUST key by `(symbol, mode)`, not `symbol` alone
`alpha:pos:{symbol}:{mode}`, `alpha:pending:{message_id}`,
`alpha:cd:{symbol}:{mode}`, `alpha:exitcd:{symbol}:{mode}`. A symbol-only
key means ACK'ing a second signal for the SAME symbol under a DIFFERENT
mode (e.g. RELIANCE GAP_FADE ACK'd, then later RELIANCE MOMENTUM ACK'd)
silently overwrites and loses the first position's exit tracking.

### Entry-alert generation — MUST guard against an already-tracked position
The entry-cooldown check alone is time-based and has NO awareness of
whether a position is still open. A position held longer than its own
cooldown window (e.g. a MOMENTUM position held 20 minutes past its 300s
cooldown) would otherwise get a duplicate, unsolicited fresh entry alert
on the next qualifying tick. Before generating any entry alert, check
whether `alpha:pos:{symbol}:{mode}` already exists for that exact mode
and skip generation if so.

### Exit-alert direction & metric (MUST be direction-aware and mode-aware)
```python
if position_mode == "GAP_FADE":
    current_exit_metric = score_data["gap_unrestricted"]   # NOT score_data["gap"] -- see Calculator #7
else:
    current_exit_metric = score_data["score"]

if position_direction == "LONG":
    should_exit = current_exit_metric <= EXIT_ALERT_SCORE_THRESHOLD
else:
    should_exit = current_exit_metric >= -EXIT_ALERT_SCORE_THRESHOLD
```
Plain `abs(score) <= threshold` is NOT sufficient — it misses "reversed
hard against the entry direction" (e.g. LONG entry at +8.5 reversing to
-6.0: `abs(-6.0)=6.0` is not `<= 2.0`, so a naive check never fires even
though the position is now moving strongly against the user). GAP_FADE's
direction comes from the gap's own sign (`determine_entry_direction`),
which can legitimately differ from the composite score's sign — this is
exactly why GAP_FADE needs its own exit metric, not the composite score.

The exit-alert TEXT must display whichever metric actually decided the
exit (gap score for GAP_FADE, composite score otherwise) — not always the
composite score, which would show a number unrelated to the real decision
for a GAP_FADE exit.

### Telegram integration
- `python-telegram-bot` v21+, async `Application`-based.
- Buttons: entry alert → `✅ ACK` / `⏭️ SKIP`; exit alert → `✔️ DONE` /
  `⏳ HOLD MORE`.
- `ACK` → move the pending Redis record to `alpha:pos:{symbol}:{mode}`
  (6h TTL). `SKIP` → log to `alpha:skip:{YYYYMMDD}` (30-day retention) for
  backtesting. `DONE` → delete the tracked position AND re-arm a fresh
  entry cooldown for `(symbol, mode)` (otherwise, if the user closes a
  position after its original cooldown already expired naturally, the
  very next qualifying tick fires an immediate duplicate entry alert for
  the trade they just closed). `HOLD MORE` → double the exit cooldown.
- Every `edit_message_text` call MUST read `message.text_html` (HTML-
  escaped) and pass `parse_mode="HTML"` — pairing the escaping with the
  matching parse mode so `&`/`<`/`>` in any future alert text render
  correctly instead of as literal escaped entities.
- Wrap every `bot.send_message`/`edit_message_text` call in `try/except`
  — a Telegram outage (down API, `RetryAfter`, network error) MUST NOT
  crash or block Process C; log and continue.
- If `TELEGRAM_TOKEN`/`TELEGRAM_CHAT_ID` are missing or the library import
  fails, disable the bot entirely and fall back to printing every alert
  to stdout (`[TG STUB ENTRY]`/`[TG STUB EXIT]`) instead of crashing.

### Async Redis client (Process C only)
`redis.asyncio.from_url(REDIS_URL, decode_responses=True,
health_check_interval=30, socket_keepalive=True)`.

---

## 9. Launcher & Shutdown

- `spawn_all_processes()` starts fresh `Process` objects for A/B/C every
  time — never reuse a dead `Process` object.
- On any single process death, terminate ALL THREE (`SIGTERM` → wait up to
  5s total → `SIGKILL` any stragglers → `join()`), then respawn all three
  together. Rationale: Processes B and C depend on A's tick stream and
  each other's ZMQ sockets; partial respawn risks orphaned connections.
- **Crash-loop breaker:** track restart timestamps in a `deque`; if more
  than `MAX_RESTARTS_ALLOWED_PER_WINDOW` (6) restarts occur within
  `RESTART_COUNTING_WINDOW_SECONDS` (3600s), print a clear diagnostic and
  `sys.exit(1)` — do not retry forever on a persistent, non-self-healing
  problem.
- **Exit-code-aware behavior:** `EXIT_CODE_FATAL_CONFIG` → stop
  immediately, no retry, no crash-loop counting (this will never fix
  itself without a human editing `.env`). `EXIT_CODE_PLANNED_RESTART` →
  respawn immediately, does NOT count as a "problem" restart for log
  messaging (though it still goes through the same respawn path).
- **ZMQ shutdown ordering (MUST close-before-term, MUST set LINGER=0):**
  ```python
  for socket in (tick_subscriber, score_publisher):   # or score_subscriber in Process C
      socket.setsockopt(zmq.LINGER, 0)   # MUST be set BEFORE close()
      socket.close()
  zmq_context.term()   # MUST be called AFTER every socket from this context is closed
  ```
  `zmq_context.term()` blocks (hangs) until every socket created from that
  context has been closed — calling `term()` before `close()` deadlocks
  the process, forcing the launcher's 5-second SIGKILL fallback on every
  shutdown. Default `LINGER=-1` (infinite) also means `close()` itself can
  block waiting to flush unsent buffered messages, reintroducing the same
  kind of hang even with the correct close-before-term ordering — set
  `LINGER=0` on every socket before closing it during shutdown.
- **Async Redis client (Process C)** MUST be closed via
  `await redis_client_async.aclose()` before the ZMQ teardown above, inside
  a `finally` block, wrapped in its own `try/except` so a Redis-close
  failure doesn't prevent the ZMQ cleanup from running.
- **Stale IPC socket files:** UNIX domain sockets (`ipc://` addresses) are
  files on disk not automatically removed by the kernel on an ungraceful
  exit (e.g. `SIGKILL`) — the next `bind()` on the same path fails with
  "Address already in use". Run a `cleanup_ipc_socket_files()` pass BOTH
  at launcher startup (defensive, handles leftovers from a prior crash)
  AND after every `shutdown_all_processes()` call.
- **Process B and C MUST register their own `SIGTERM`/`SIGINT` handlers**
  (Process C via `asyncio`'s `add_signal_handler`, wrapped in
  `try/except NotImplementedError` for platforms where it's unavailable)
  so `process.terminate()` triggers an orderly shutdown through their own
  `finally` blocks rather than an abrupt kill mid-operation.

---

## 10. Poll/Receive Timeouts (MUST NOT block forever)

Both Process B's tick-receive loop and Process C's score-receive loop MUST
use a timeout-based receive (a `zmq.Poller` with a timeout in B; an
`asyncio.wait_for(..., timeout=SCORE_RECEIVE_TIMEOUT_SECONDS)` wrapper
around `recv_multipart()` in C), NOT a plain blocking `recv_multipart()`.
A plain blocking receive that never times out means the loop never reaches
its periodic stats-print or stop-signal check while upstream is quiet
(market closed, upstream process down) — the console goes silent with zero
indication of *why*, indistinguishable from an actual crash. With a
timeout, both processes keep printing periodic stats (`ticks=0` / "no
scores received in last Ns") even during genuine silence, and the stop
signal is checked promptly instead of only after the next message arrives.

---

## 11. Companion Files (keep, do not delete as "extra")

- `alpha_decay_lite.py` — everything above.
- `diag_bidask.py` — standalone diagnostic script that subscribes one
  symbol during market hours, prints several depth snapshots, and reports
  whether the installed Angel SDK version swaps `best_5_buy_data`/
  `best_5_sell_data` (to inform the `ANGEL_BIDASK_SWAPPED` env flag).
- `requirements.txt` — pin `pyzmq`, `redis`, `python-dotenv`,
  `python-telegram-bot>=21.0`, `smartapi-python`, plus its transitive
  deps explicitly (`logzero`, `websocket-client`, `pyotp`, `requests`) since
  upstream's own `setup.py` is incomplete; `psycopg[binary]` only if/when
  TimescaleDB logging is added.
- `.env.example`, `.gitignore` (excl. `.env`, `.venv`, logs, ZMQ ipc
  sockets, Redis dump files) — genuinely necessary, not "extra".
- `tick_store/` — a SEPARATE, standalone, broker-agnostic tick-capture
  service (own README/requirements/.env.example), used to independently
  record historical tick data to a local SQLite DB with a FastAPI query
  layer, designed to run on cheap single-core VPS boxes independent of
  this alerting engine. Not part of the alerting pipeline itself — do not
  merge its logic into `alpha_decay_lite.py`.

---

## 12. Explicitly Rejected / Out of Scope (do not implement without a fresh product decision)

- **SEBI algo approval, paper-trading gate, hard SL infra, position
  reconciliation, MIS auto-square-off** — all user's manual responsibility,
  never implement automated order placement of any kind.
- **Global Telegram alert rate limiting** ("max N/hour") — explicitly
  removed; advisory system, user decides which trades to take.
- **A general architecture rewrite to asyncio-everywhere or 100+
  symbols/5000+ ticks-sec scale** — current design targets ~50-100 liquid
  NSE symbols at ~1-2k ticks/sec; do not rewrite the core multiprocessing
  + threading model to chase a scale target that hasn't been requested.
- **orjson/msgpack serialization, monotonic-queue min/max for the
  absorption calculator's 30s price window, volatility-adaptive EMA** —
  legitimate micro-optimizations, but the current deque sizes at this
  scale make them premature; do not add without profiling data showing
  they matter.
- **Persisting `SymbolState` to Redis across restarts** — a real
  limitation (RVOL/EMA/peak state resets on any Process B restart,
  including the scheduled ~20h JWT-refresh restart), documented as an
  accepted trade-off rather than implemented, since it adds real latency
  and complexity for an event that happens roughly once per trading day.
- **Making GAP_FADE track real-time gap-fill via `last_traded_price`**
  instead of the static `open_price` — a meaningfully different signal
  definition (not a bug fix), requires an explicit product decision first.

---

## 13. Verification Checklist (apply before considering any of the above "done")

For every calculator/fix above, before shipping: write a standalone
Python simulation reproducing the EXACT before/after behavior (not just
"looks right on inspection"), run `python3 -m py_compile` for a syntax
check, and re-trace every dependent code path the change touches (e.g. a
change to `compute_feature_score`'s return signature touches every call
site and every published-score consumer). Do not accept a pasted review's
claims at face value — trace the actual code and, where possible, prove
the claimed failure mode with a runnable simulation before writing a fix,
and be willing to explicitly reject a claim (with the disproof shown) when
the simulation contradicts it.
