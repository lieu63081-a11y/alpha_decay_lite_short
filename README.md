# Alpha-Decay Lite

Signal-only NSE cash equity alerting engine. Advisory only — no order placement.

Angel One SmartAPI से live market data लो, 60-second sliding window पर score compute करो, high-quality Buy/Sell alerts Telegram पर भेजो. User manually order place करता है broker app पर.

> **Design contract:** यह system कभी order fire नहीं करता. सिर्फ़ actionable signals deliver करता है. सारा execution risk और final trade decision user के पास है. **Algo advisory है, algo execution नहीं.**

> **Naming note:** every variable and function in `alpha_decay_lite.py` uses a full, descriptive name (no single-letter or cryptic abbreviations) — e.g. `symbol_state` not `st`, `last_traded_price` not `ltp`, `calculate_relative_volume` not `_c2_rvol`. This document uses those same full names throughout.

---

## Architecture (3 processes)

```
Angel WS (mode 3)  →  data_factory()   ─ZMQ ticks─→   alpha_engine()   ─ZMQ scores─→  broadcaster_loop()  →  Telegram
                       (Process A)                     (Process B)                    (Process C)
                                                        9 calculators + EMA smoother   Telegram bot + cooldowns + exit tracking
```

| Process | Entry point | Role | Heartbeat |
|---|---|---|---|
| **A — Data Factory** | `data_factory()` | Angel `SmartWebSocketV2` mode 3, publishes ticks over ZMQ | `alpha:hb:A` + `alpha:hb:data` |
| **B — Alpha Engine** | `alpha_engine()` | 9 calculators + trade classification + EMA smoother | `alpha:hb:B` |
| **C — Broadcaster** | `run_broadcaster_process()` → `broadcaster_loop()` | Telegram bot, cooldowns, exit-alert tracking | `alpha:hb:C` |

A top-level launcher (in `if __name__ == "__main__":`) starts all three via `spawn_all_processes()`, monitors them, and automatically respawns all three if any one dies (self-healing, with a crash-loop breaker capped at `MAX_RESTARTS_ALLOWED_PER_WINDOW`).

End-to-end latency: **~1 ms** tick-to-score-published, ~200 ms tick-to-Telegram (network dominant).

---

## 9 Calculators (all amortized O(1) per tick)

| # | Function | Role |
|---|----------|------|
| 1 | `calculate_vwap_distance`             | Last-traded-price % distance from the daily VWAP (mean-reversion setup) |
| 2 | `calculate_relative_volume`           | 60s volume vs a 20-min baseline that **excludes itself**, gated by a 5-min warmup |
| 3 | `calculate_aggressive_buy_pressure`   | **Rolling 30s** net aggressive buy/sell (Lee-Ready + tick-rule fallback) |
| 4 | `calculate_absorption`                | **Trade-classified**, symmetric: +1.5 sell-absorb (bullish) / −1.5 buy-absorb (bearish trap) |
| 5 | `calculate_book_imbalance`            | Order-book depth imbalance across Angel's 5-level totals |
| 6 | `calculate_spread_ratio`              | Bid-ask spread filter (> 0.15% → score gated to 0) |
| 7 | `calculate_gap_fade`                  | Gap-fade signal, active only in the 9:15–9:30 IST window |
| 8 | `calculate_session_weight`            | OPEN_VOL 1.2 · MID_QUIET 0.8 · CLOSE_HOUR 1.1 (IST) |
| 9 | EMA smoother (inline in `alpha_engine()`) | `alpha = 1 − exp(−dt/tau)`, tau=1s; first-tick handled separately; same-timestamp batched ticks preserve the previous score |

**Combination logic (inside `compute_feature_score`):**
```
combined_flow_score = max(aggressive_buy_score, absorption_score)
                     + 0.4 * min(aggressive_buy_score, absorption_score)   [if same sign -- dampen redundancy]
                     = aggressive_buy_score + absorption_score              [if opposite signs -- additive net]

composite_score = clip(
    session_weight * (2.0*vwap_distance_score + 1.5*combined_flow_score
                       + 1.5*book_imbalance_score + 1.0*gap_fade_raw_score),
    -10, +10
)  →  EMA smoother  →  smoothed_ema_score
```

---

## Signal Modes

Decided by `determine_signal_mode(composite_score, relative_volume, gap_fade_raw_score)`:

| Mode | Trigger | Cooldown | Suggested SL |
|------|---------|----------|---------------|
| `GAP_FADE`       | \|gap_fade_raw_score\| ≥ 2.0 (only nonzero 9:15–9:30 IST) | 180 s | gap-based |
| `MOMENTUM`       | \|composite_score\| ≥ 8 and relative_volume ≥ 1.5          | 300 s | 1.5% |
| `MEAN_REVERSION` | 3 ≤ \|composite_score\| < 8 and relative_volume < 2.0       | 45 s  | 0.5% |

GAP_FADE is checked first: its direction comes from the gap's own sign (`determine_entry_direction`), which can legitimately differ from the composite score's sign. This is also why the exit-alert logic evaluates GAP_FADE positions against `gap_fade_raw_score`, not the composite score — using the composite score there was a real bug that has since been fixed (see commit history).

---

## Kill Switches (5 layers)

| # | Layer | Mechanism | Result on trigger |
|---|-------|-----------|-------------------|
| 1 | **WS data flow** | `alpha:hb:data` (5s TTL, refreshed only while ticks are actually arriving in Process A) | `no_data` suppression |
| 2 | **Alpha Engine alive** | `alpha:hb:B` (3s TTL, refreshed by Process B's heartbeat thread) | `engine_dead` suppression |
| 3 | **Manual mute** | Redis `alpha:mute` **OR** disk `MUTE_FILE` (fail-safe OR — either alone is enough to mute) | `muted` suppression |
| 4 | **Spread filter** | Per-symbol `calculate_spread_ratio() > SPREAD_RATIO_MAX` gates the composite score to 0 | Score=0, no alert |
| 5 | **Per-signal cooldown** | Per (symbol, mode) via Redis `SET NX EX` — prevents the SAME signal from firing repeatedly | Duplicate suppressed |

> **No global rate limit, by design.** This is an advisory system — the user decides which alerts to act on, so every qualifying signal is delivered. Cooldowns exist only to stop one signal (e.g. RELIANCE MOMENTUM) from re-firing every tick while its score stays inside the trigger band.

Suppression status is visible in every `[C]` stats line as `suppressed=<reason>`.

---

## Telegram Bot Integration

### Entry alert (with buttons)

```
🔔 [MOMENTUM] RELIANCE LONG
score=+8.42  peak=8.67  ltp=1289.50
suggested SL: 1.5%
advisory only — user executes manually

[✅ ACK]    [⏭️ SKIP]
```

- **ACK** → position saved to Redis (`alpha:pos:{symbol}`, 6h TTL) with its `direction` recorded explicitly (needed for correct exit-direction handling, especially for GAP_FADE). Place the order manually on your broker's app.
- **SKIP** → logged to `alpha:skip:{YYYYMMDD}` for later backtesting, 30-day retention.

### Exit alert (auto-fires when an ACK'd position's score crosses back through the exit threshold)

```
⚠️ EXIT — RELIANCE (MOMENTUM LONG)
score decayed to +1.82 (entry +8.42)
ltp 1295.30 (entry 1289.50, delta +0.45%)
consider closing position

[✔️ DONE]  [⏳ HOLD MORE]
```

The exit condition is **direction-aware**, not a plain `abs(score) <= threshold` check: for a LONG position it fires when the score falls to or below `+EXIT_ALERT_SCORE_THRESHOLD`, and for a SHORT position when the score rises to or above `-EXIT_ALERT_SCORE_THRESHOLD`. This correctly catches both "decayed back to flat" AND "reversed hard against the entry direction" — a plain absolute-value check would miss the second case entirely.

- **DONE** → position deleted, no further exit alerts for it.
- **HOLD MORE** → the exit cooldown is doubled; wait for further decay before the next exit alert.

### Persistence (Redis keys)

- `alpha:pending:{message_id}` (24h TTL) — entry details, kept until ACK/SKIP. Survives a process restart.
- `alpha:pos:{symbol}` (6h TTL) — the active tracked position, including its saved `direction`. Auto-cleanup after 6h.
- `alpha:exitcd:{symbol}` (5 min TTL, doubled by HOLD MORE) — prevents exit-alert spam.

---

## Quick Start

### 1. Prerequisites

- Linux (Ubuntu 24.04 LTS recommended) with Python 3.10+. **Not Windows** — the code relies on `time.tzset()`, which is Unix-only, and fails fast with a clear error if run elsewhere.
- Redis 7 running locally.
- An Angel SmartAPI account: API key, client code, 4-digit MPIN, TOTP secret.
- A Telegram bot from `@BotFather` plus your chat ID.

### 2. Install

```bash
sudo apt install -y python3-pip python3-venv python3-dev redis-server git
sudo systemctl enable --now redis-server

git clone https://github.com/lieu63081-a11y/alpha_decay_lite_short.git
cd alpha_decay_lite_short
python3 -m venv .venv && source .venv/bin/activate
pip install --upgrade pip wheel && pip install -r requirements.txt
```

### 3. Configure

```bash
cp .env.example .env && nano .env
sudo timedatectl set-ntp true    # critical for TOTP to generate valid codes
```

**Required environment variables:**

| Var | Value | Notes |
|---|---|---|
| `ANGEL_API_KEY` | SmartAPI key | from `smartapi.angelbroking.com` |
| `ANGEL_CLIENT_CODE` | e.g. `P12345678` | your client ID |
| `ANGEL_PASSWORD` | **4-digit MPIN** | NOT your web login password |
| `ANGEL_TOTP_SECRET` | base32 secret string | the full secret, not the 6-digit code it generates |
| `ANGEL_BIDASK_SWAPPED` | `0` (default) or `1` | only set to `1` after `diag_bidask.py` confirms a swap on your SDK version |
| `TELEGRAM_TOKEN` | bot token | from `@BotFather` |
| `TELEGRAM_CHAT_ID` | your chat ID | get it from `@userinfobot` |
| `UNIVERSE` | comma-separated symbols | e.g. `RELIANCE,TCS,YESBANK,ZOMATO` |
| `REDIS_URL` | default `redis://localhost:6379/0` | |
| `MUTE_FILE` | default `/tmp/alpha_mute.flag` | |
| `AUTO_RESTART_HOURS` | default `20` | scheduled self-exit before the Angel JWT expires; the launcher respawns automatically |

### 4. Run

```bash
python alpha_decay_lite.py
```

The launcher stays in the foreground, monitoring and respawning Processes A/B/C as needed. Use `nohup`, `tmux`, `screen`, or a systemd unit (below) to keep it running after you disconnect.

### 5. (Optional) Verify the bid/ask field order

Angel's SDK has been reported to swap `best_5_buy_data`/`best_5_sell_data` on some versions, which silently disables the spread filter. Verify once, during market hours:

```bash
python diag_bidask.py
```

It subscribes RELIANCE, prints 5 depth snapshots, and tells you whether to set `ANGEL_BIDASK_SWAPPED=1`.

---

## Expected Output

**Startup:**
```
[B] Alpha Engine up
[C] Broadcaster up
[C] Telegram bot up (chat=123456789)
[A] Login OK, client=P12345678
[A] Scrip master loaded in 12.3s, resolved 5/5 symbols
[A] Data Factory up, 5 tokens mode=3
```

On your Telegram: `🟢 Alpha-Decay Lite bot online`

**Live stats (Process B every 10s, Process C every 15s):**
```
[B] ticks=87 (8/s)  syms=5  top: RELIANCE=+0.42(ltp=1289.5)  TCS=-0.18(ltp=4102.0)  ...
[C] {'scores_received': 130, 'no_qualifying_mode': 122, 'muted': 0, 'no_data': 0,
     'engine_dead': 0, 'cooldown_blocked': 5, 'entry_alerts_sent': 3,
     'exit_alerts_sent': 0, 'redis_errors': 0}
```

If no ticks or scores arrive within the poll/receive timeout, both processes still print their periodic stats (showing `ticks=0` or `no scores received in last Ns`) instead of going silent — a silent console with no output for minutes at a time indicates a real crash, not normal quiet-market behavior.

**When alerts fire:** the message lands on your phone; tap the buttons.

---

## Operations

### Emergency mute (dual-write, fail-safe OR)

```bash
redis-cli set alpha:mute 1        # either one alone is enough
touch /tmp/alpha_mute.flag
```

Unmute:
```bash
redis-cli del alpha:mute && rm -f /tmp/alpha_mute.flag
```

### Inspect state

```bash
redis-cli keys 'alpha:*'
watch -n 2 'redis-cli --scan --pattern "alpha:pos:*"'   # active tracked positions
redis-cli lrange "alpha:skip:$(date +%Y%m%d)" 0 -1     # today's skipped signals
```

### Health monitoring

```bash
redis-cli get alpha:hb:A       # Process A alive (returns "1", TTL 1-3s)
redis-cli get alpha:hb:B       # Process B alive
redis-cli get alpha:hb:C       # Process C alive
redis-cli get alpha:hb:data    # data flowing (only set while ticks are recent)
```

### Graceful shutdown

Press Ctrl+C, or send SIGTERM (`kill <pid>` on the launcher process). Both Process B and Process C have their own signal handlers, so they exit cleanly through their normal shutdown paths (closing ZMQ sockets, stopping the Telegram bot) rather than being killed mid-operation. The top-level launcher then removes any leftover ZMQ IPC socket files before exiting.

### systemd service (optional — the built-in self-healing launcher works without it)

```ini
# /etc/systemd/system/alpha-decay.service
[Unit]
Description=Alpha-Decay Lite Signal Engine
After=network-online.target redis-server.service
Requires=redis-server.service

[Service]
Type=simple
User=alpha
WorkingDirectory=/home/alpha/alpha_decay_lite_short
EnvironmentFile=/home/alpha/alpha_decay_lite_short/.env
Environment=PYTHONUNBUFFERED=1
ExecStart=/home/alpha/alpha_decay_lite_short/.venv/bin/python alpha_decay_lite.py
Restart=on-failure
RestartSec=5
StandardOutput=append:/var/log/alpha-decay.log
StandardError=append:/var/log/alpha-decay.log

[Install]
WantedBy=multi-user.target
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ModuleNotFoundError` | Virtualenv dependencies missing | `pip install -r requirements.txt` |
| `Invalid password parameter name` | Web login password used instead of MPIN | Set `ANGEL_PASSWORD=<4-digit-MPIN>` |
| `Invalid Token` / TOTP failure | System clock drift | `sudo timedatectl set-ntp true` |
| Silent pause after "in pool" | Scrip master (~4 MB) downloading | Wait 30-60s |
| Bot not sending messages | `TELEGRAM_TOKEN` or `TELEGRAM_CHAT_ID` missing/wrong | `grep TELEGRAM .env` to verify |
| `[C] suppressed=engine_dead` | Process B is down or stuck | Check `[B]` output; the launcher should auto-respawn |
| `[C] suppressed=no_data` | Angel WS disconnected, or market is closed | Wait for reconnect / market hours |
| `redis_errors` counter growing | Redis unreachable | `systemctl status redis-server` |
| Alerts stopped, then resumed on their own | Scheduled JWT-refresh restart (`AUTO_RESTART_HOURS`) | Expected behavior — the launcher respawns all processes automatically |
| Script exits with a crash-loop message | More than `MAX_RESTARTS_ALLOWED_PER_WINDOW` restarts within an hour | Real underlying problem (bad credentials, network) — check the printed error before the crash |
| Runs on Windows and produces wrong session-window signals... or refuses to start | `time.tzset()` is Unix-only | Run on Linux; the script fails fast with a clear message on non-POSIX platforms instead of silently miscalculating IST windows |

---

## Known Limitations (by design, not bugs to silently ignore)

These are architectural trade-offs, documented here so they are not mistaken for
oversights. Each has a real cost; each was also evaluated against the cost of
fixing it properly, and left as-is for now.

- **All in-memory calculator state is lost on any Process B restart.**
  `symbol_state_by_symbol` (every symbol's 20-minute RVOL baseline, 30-second
  aggression window, EMA score, peak amplitude) lives only in Process B's
  memory. A crash, a manual restart, or the scheduled JWT-refresh restart
  (`AUTO_RESTART_HOURS`, which respawns **all three** processes together)
  wipes it. After a restart, RVOL returns to its neutral warmup value of `1.0`
  for the next 5 minutes (`RVOL_WARMUP_SECONDS`), and the EMA score restarts
  from each symbol's very next tick — meaning MOMENTUM/MEAN_REVERSION/GAP_FADE
  cannot fire on old context for a few minutes after every restart. A fix
  would mean persisting `SymbolState` to Redis and reloading it on startup,
  which adds real latency and complexity for a restart event that, at the
  default `AUTO_RESTART_HOURS=20`, happens once roughly every 20 hours (i.e.
  typically once per trading day, usually outside active hours). Not
  implemented; noted here so it's an informed trade-off, not a surprise.

- **No global Telegram rate limit.** Cooldowns are per (symbol, mode), not
  system-wide (see "No global rate limit, by design" above). In a scenario
  where 20-30+ symbols cross a trigger threshold within the same second (a
  sharp, broad market-open move), `send_entry_alert` calls are issued
  independently and could hit Telegram's own rate limits
  (`telegram.error.RetryAfter`), silently dropping some alerts. In practice
  this requires a genuinely unusual number of simultaneous qualifying
  signals; the fix (a shared async semaphore + backoff/retry queue in front
  of every `bot.send_message` call) is a reasonable future addition but adds
  its own latency and complexity for a low-frequency event.

- **`RollingWindowSum.running_sum` accumulates floating-point rounding error
  over very long uptimes.** Every `add()` does a float `+=`/`-=`; volumes are
  currently integers cast to float, so this is inert today, but if a future
  calculator fed genuinely fractional values (e.g. raw price) through this
  same class over a multi-day uptime, `running_sum` could drift to a small
  nonzero value (like `1e-10`) even when the window is logically empty. Not
  an issue for the current volume-only usage; worth remembering if this class
  is reused for price/VWAP data later.

- **A tick that arrives with the exact same timestamp as the previous tick
  for that symbol contributes to `update_rolling_windows` but NOT to the EMA
  score.** This can happen when the exchange feed delivers two updates in the
  same millisecond (batched packets). The composite score for that
  duplicate-timestamp tick is computed but discarded rather than blended into
  `smoothed_ema_score` — see the `elif tick_data["timestamp"] >
  symbol_state.last_tick_timestamp:` branch in `alpha_engine()`. In the rare
  case where the discarded tick represented a materially different market
  state (e.g. a large aggressive order), that specific signal is smoothed
  away rather than acted on. A fix would need a tie-breaking sequence number
  from the exchange feed (Angel's mode-3 payload does not reliably provide
  one) rather than relying on wall-clock time alone.

- **Mute status is cached for `MUTE_STATUS_CACHE_SECONDS` (1 second).** If the
  user mutes via Telegram/Redis/disk-flag at the exact moment a qualifying
  signal is being evaluated inside that 1-second cache window, the alert can
  still go out before the mute is picked up on the next cache refresh. This
  is a deliberate latency/Redis-load trade-off (checking Redis + disk on
  every single incoming score message, hundreds of times per second at
  scale, is unnecessary overhead) rather than an oversight. Worst case is a
  roughly 1-second-late mute, which is acceptable for an advisory system
  where the user still makes the final manual trading decision.

---

## Roadmap

- [x] 3-process pipeline with real Angel WebSocket integration
- [x] 9 calculators with amortized O(1) rolling windows
- [x] Cooldowns per (symbol, mode) + mute (dual-write, fail-safe OR)
- [x] Spread filter kill switch
- [x] Live stats prints with receive-timeout fallback (never goes silent)
- [x] IST timezone forcing (`TZ` env + `tzset()`), with a Windows fail-fast guard
- [x] RVOL warmup gate + self-exclusion from its own baseline
- [x] Time-based peak-amplitude decay (60s half-life)
- [x] JWT auto-refresh (scheduled restart before expiry)
- [x] Self-healing launcher: automatic respawn of all 3 processes on any crash, with a crash-loop breaker
- [x] Graceful per-process shutdown (SIGTERM handlers in every process)
- [x] Kill switches #1 (WS data-flow heartbeat) + #2 (engine-alive heartbeat) + #3 (dual-write mute)
- [x] Telegram bot with ACK / SKIP / DONE / HOLD MORE inline buttons
- [x] Exit alerts, direction-aware (handles both decay-to-flat and hard reversal)
- [x] GAP_FADE mode with its own entry direction and exit-condition logic (independent of the composite score)
- [x] Trade-classified absorption (Lee-Ready + tick-rule fallback)
- [x] Rolling 30-second aggression window (replaces noisy per-tick classification)
- [x] Full descriptive naming throughout the codebase (no abbreviations)
- [x] RVOL warmup-window inflation fix (baseline divisor uses actual elapsed minutes, not a hardcoded 19.0)
- [x] Monotonic cumulative-volume watermark (rejects out-of-order/stale ticks instead of corrupting the delta calculation)
- [x] FATAL config errors (missing/invalid credentials) stop the launcher instead of endlessly respawning
- [x] Telegram HTML formatting paired with `parse_mode="HTML"` on every button-callback edit
- [x] Defensive `.get()` access on all Redis-stored dict fields (no KeyError risk on partial/legacy records)
- [x] Explicit ZMQ context + async Redis connection cleanup on graceful shutdown
- [ ] TimescaleDB async logger for tick + score history
- [ ] Scrip master disk cache with mtime-based refresh
- [ ] Backtest harness against 3-month historical tick data
- [ ] LUNCH_HOUR sub-band (12:00-13:30 IST) tuning

---

## Environment

- Python 3.10+ (Ubuntu 24.04 ships 3.12 by default)
- Redis 7+ (AOF everysec recommended)
- `python-telegram-bot` v21+ (async, `Application`-based)
- Linux/Unix only (see Prerequisites above)

## Files

- `alpha_decay_lite.py` — the 3-process pipeline (Data Factory, Alpha Engine, Broadcaster + Telegram bot, self-healing launcher)
- `diag_bidask.py` — standalone diagnostic to check whether your Angel SDK version swaps bid/ask depth labels
- `requirements.txt` — Python dependencies (Angel SDK transitive deps pinned explicitly)
- `.env.example` — environment variable template
- `.gitignore` — excludes `.env`, `.venv`, logs, ZMQ IPC sockets, Redis dump files
- `tick_store/` — standalone tick-capture service for historical data collection (see `tick_store/README.md`)
