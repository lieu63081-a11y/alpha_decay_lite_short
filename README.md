# Alpha-Decay Lite

Signal-only NSE cash equity alerting engine. Advisory only — no order placement.

Angel One SmartAPI से live market data लो, 60-second sliding window पर score compute करो, high-quality Buy/Sell alerts Telegram पर भेजो. User manually order place करता है broker app पर.

> **Design contract:** यह system कभी order fire नहीं करता. सिर्फ़ actionable signals deliver करता है. सारा execution risk और final trade decision user के पास है. **Algo advisory है, algo execution नहीं.**

---

## Architecture (3 processes)

```
Angel WS (mode 3)  →  Process A  ─ZMQ ticks─→   Process B  ─ZMQ scores─→  Process C  →  Telegram
                       Data Factory              Alpha Engine               Broadcaster
                                                 9 calc + EMA smoother      dedup + rate-limit + bot
```

| Process | Role | Heartbeat |
|---|---|---|
| **A — Data Factory** | Angel `SmartWebSocketV2` mode 3, ZMQ ipc publish | `alpha:hb:A` + `alpha:hb:data` |
| **B — Alpha Engine** | 9 calculators + trade classification + EMA smoother | `alpha:hb:B` |
| **C — Broadcaster** | Telegram bot + dedup (cooldowns) + exit tracking | — |

End-to-end latency: **~1 ms** tick-to-score-published, ~200 ms tick-to-Telegram (network dominant).

---

## 9 Calculators (all amortized O(1) per tick)

| # | Function | Role |
|---|----------|------|
| 1 | `_c1_vwap_distance`  | LTP % distance from daily VWAP (mean-reversion setup) |
| 2 | `_c2_rvol`           | 60s vol vs 20-min baseline **excluding self**, warm-up gated at 5 min |
| 3 | `_c3_agg_buy`        | **Rolling 30s** net aggressive buy/sell (Lee-Ready + tick-rule) |
| 4 | `_c4_absorption`     | **Trade-classified** symmetric absorption: +1.5 sell-absorb / −1.5 buy-absorb |
| 5 | `_c5_imbalance`      | L5 book depth imbalance |
| 6 | `_c6_spread`         | Spread filter (> 0.15% → score gated to 0) |
| 7 | `_c7_gap`            | Gap fade in 9:15–9:30 IST window |
| 8 | `_c8_session_weight` | OPEN_VOL 1.2 · MID_QUIET 0.8 · CLOSE_HOUR 1.1 (IST) |
| 9 | EMA smoother         | `α = 1 − exp(−dt/τ)`, τ=1s; first-tick init separated; batched ticks preserve prev |

**Aggregator:**
```
hidden = max(agg,absb) + 0.4·min(agg,absb)   [if same sign — dampen redundancy]
       = agg + absb                          [if opposite — additive net]
raw    = session_w × (2·vwap + 1.5·hidden + 1.5·imb + 1.0·gap)
score  = clip(raw, −10, +10)  →  EMA smoother
```

---

## Signal Modes

| Mode | Trigger | Cooldown | SL |
|------|---------|----------|-----|
| `MOMENTUM`       | \|score\| ≥ 8, rvol ≥ 1.5 | 300 s | 1.5% |
| `MEAN_REVERSION` | 3 ≤ \|score\| < 8, rvol < 2.0 | 45 s | 0.5% |
| `GAP_FADE`       | 9:15–9:30 IST, \|gap\| > 1% *(planned)* | 180 s | gap-based |

---

## Kill Switches (5 layers)

| # | Layer | Mechanism | Result on trigger |
|---|-------|-----------|-------------------|
| 1 | **WS data flow** | `alpha:hb:data` (5s TTL, refreshed only on live ticks in Process A) | `no_data` suppress |
| 2 | **Alpha Engine alive** | `alpha:hb:B` (3s TTL, refreshed by Process B heartbeat thread) | `engine_dead` suppress |
| 3 | **Manual mute** | Redis `alpha:mute` **OR** disk `MUTE_FILE` (fail-safe OR) | `muted` suppress |
| 4 | **Spread filter** | Per-symbol `spread > 0.15%` gates score to 0 | Score=0, no alert |
| 5 | **Per-signal cooldown** | per (sym, mode) via Redis `SET NX EX` — prevents duplicate of same signal | Duplicate suppressed |

> **No global rate limit by design.** This is an advisory system — user decides which alerts to trade. All qualifying signals are delivered. Cooldowns still prevent the SAME signal (e.g. RELIANCE MOMENTUM) from re-firing while score stays in trigger band.

Suppression status visible in every `[C]` stats line as `suppressed=<reason>`.

---

## Telegram Bot Integration

### Entry Alert (with buttons)

```
🔔 [MOMENTUM] RELIANCE LONG
score=+8.42  peak=8.67  ltp=1289.50
suggested SL: 1.5%
advisory only — user executes manually

[✅ ACK]    [⏭️ SKIP]
```

- **ACK** → position tracked in Redis (`alpha:pos:{sym}`, 6h TTL). Broker पर manually order place करें.
- **SKIP** → logged in `alpha:skip:{YYYYMMDD}` for backtest, 30-day retention.

### Exit Alert (auto-fires when ACK'd position's score decays to ±2)

```
⚠️ EXIT — RELIANCE (MOMENTUM LONG)
score decayed to +1.82 (entry +8.42)
ltp 1295.30 (entry 1289.50, delta +0.45%)
consider closing position

[✔️ DONE]  [⏳ HOLD MORE]
```

- **DONE** → position deleted, no more exit alerts.
- **HOLD MORE** → exit cooldown doubles (10 min); wait for further decay.

### Persistence

- `alpha:pending:{message_id}` (24h TTL) — stores entry details until ACK/SKIP. Survives process restart.
- `alpha:pos:{sym}` (6h TTL) — active tracked position. Auto-cleanup after 6h.
- `alpha:exitcd:{sym}` (5 min TTL) — prevents exit alert spam.

---

## Quick Start

### 1. Prerequisites

- Ubuntu 24.04 LTS (or Linux with Python 3.10+)
- Redis 7 running locally
- Angel SmartAPI account (API key, client code, 4-digit MPIN, TOTP secret)
- Telegram bot from `@BotFather` + your chat ID

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
sudo timedatectl set-ntp true    # critical for TOTP
```

**Required env vars:**

| Var | Value | Notes |
|---|---|---|
| `ANGEL_API_KEY` | SmartAPI key | from `smartapi.angelbroking.com` |
| `ANGEL_CLIENT_CODE` | e.g. `P12345678` | your client ID |
| `ANGEL_PASSWORD` | **4-digit MPIN** | NOT web login password |
| `ANGEL_TOTP_SECRET` | base32 secret | full string, not 6-digit code |
| `TELEGRAM_TOKEN` | bot token | from `@BotFather` |
| `TELEGRAM_CHAT_ID` | chat ID | get from `@userinfobot` |
| `UNIVERSE` | comma-separated symbols | e.g. `RELIANCE,TCS,YESBANK,ZOMATO` |
| `REDIS_URL` | default `redis://localhost:6379/0` | |
| `MUTE_FILE` | default `/tmp/alpha_mute.flag` | |
| `AUTO_RESTART_HOURS` | default `20` | JWT auto-refresh; systemd restarts process |

### 4. Run

```bash
python alpha_decay_lite.py
```

Or use the update helper:

```bash
./update.sh --run    # git pull + pip install + launch
```

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

आपके Telegram पर: `🟢 Alpha-Decay Lite bot online`

**Live stats (every 10s from B, 15s from C):**
```
[B] ticks=87 (8/s)  syms=5  top: RELIANCE=+0.42(ltp=1289.5)  TCS=-0.18(ltp=4102.0)  ...
[C] {'scores': 130, 'no_mode': 130, 'muted': 0, 'no_data': 0, 'engine_dead': 0, 'cooldown': 0, 'sent': 0, 'exit_sent': 0, 'redis_err': 0}
```

**When alerts fire:** message आपके phone पर, buttons पर tap करें.

---

## Operations

### Emergency mute (dual-write, fail-safe OR)

```bash
redis-cli set alpha:mute 1        # या
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
redis-cli get alpha:hb:A       # Process A alive (should return "1", TTL 1-3s)
redis-cli get alpha:hb:B       # Process B alive
redis-cli get alpha:hb:data    # Data flowing (only if ticks recent)
```

### systemd service

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
| `ModuleNotFoundError` | Venv deps missing | `pip install -r requirements.txt` |
| `Invalid password parameter name` | Web password instead of MPIN | Set `ANGEL_PASSWORD=<4-digit-MPIN>` |
| `Invalid Token` / TOTP fail | Clock drift | `sudo timedatectl set-ntp true` |
| Silent hang after "in pool" | Scrip master downloading | Wait 30-60s |
| Bot not sending | `TELEGRAM_TOKEN` or `TELEGRAM_CHAT_ID` missing/wrong | Verify with `grep TELEGRAM .env` |
| `[C] suppressed=engine_dead` | Process B down or stuck | Check `[B]` output; restart if needed |
| `[C] suppressed=no_data` | Angel WS disconnected or market closed | Wait for reconnect / market hours |
| `[C] {'redis_err': N}` | Redis down or unreachable | Check `systemctl status redis-server` |
| Alerts stopped after 20h | JWT auto-refresh triggered | Systemd will auto-restart; else run again |

---

## Roadmap

- [x] 3-process pipeline scaffold with real Angel WS integration
- [x] 9 calculators with amortized O(1) rolling windows
- [x] Cooldowns per (sym, mode) + mute (dual-write fail-safe OR)
- [x] Spread filter kill switch
- [x] Live stats prints (tick rate, top scores, broadcaster counters)
- [x] IST timezone forcing (TZ env + tzset for C-level fast path)
- [x] RVOL warmup gate + self-exclusion (correct 5x = 5.0 exact)
- [x] Time-based peak decay (60s half-life)
- [x] JWT auto-refresh (20h scheduled restart)
- [x] Graceful shutdown with watchdog (SIGTERM → 5s → SIGKILL)
- [x] Kill switches #1 (WS 5s heartbeat) + #2 (Redis TTL 3s heartbeat)
- [x] **Telegram bot with ACK/SKIP/DONE/HOLD inline buttons**
- [x] **Exit alerts when ACK'd position score decays to ±2**
- [x] **Trade-classified absorption (Lee-Ready + tick-rule)**
- [x] **Rolling 30s aggression window (replaces per-tick noise)**
- [ ] GAP_FADE mode trigger in `mode_for()`
- [ ] TimescaleDB async logger for tick + score history
- [ ] Scrip master disk cache with mtime check
- [ ] Backtest harness against 3-month historical tick data
- [ ] LUNCH_HOUR sub-band (12:00-13:30 IST) tuning

---

## Environment

- Python 3.10+ (Ubuntu 24.04 default is 3.12)
- Redis 7+ (AOF everysec recommended)
- `python-telegram-bot` v21+ (async, `Application`-based)

## Files

- `alpha_decay_lite.py` — 3-process pipeline (~625 lines)
- `requirements.txt` — Python deps (SmartAPI transitive deps pinned)
- `.env.example` — env template
- `.gitignore` — excludes `.env`, `.venv`, logs, IPC sockets, Redis dumps
- `update.sh` — one-shot pull + deps refresh + launch
