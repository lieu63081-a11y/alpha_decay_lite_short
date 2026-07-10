# Alpha-Decay Lite

Signal-only NSE cash equity alerting engine. Advisory only — no order placement.

Angel One SmartAPI se live market data lo, 60-second sliding window par score compute karo,
high-quality Buy / Sell alerts Telegram par bhejo. User manually order place karta hai broker app par.

> **Design contract:** Yeh system kabhi order fire nahi karta. Sirf actionable signals deliver karta hai.
> Saara execution risk aur final trade decision user ke paas hai. **Algo advisory hai, algo execution nahi.**

---

## Architecture (3 processes)

```
Angel WS (mode 3)  ->  Process A  --ZMQ ticks-->   Process B  --ZMQ scores-->  Process C  ->  Telegram
                       Data Factory                Alpha Engine                 Broadcaster
                                                   9 calc + EMA smoother        dedup + rate-limit + async Redis
```

| Process | Role | Tech |
|---|---|---|
| **A — Data Factory** | Angel `SmartWebSocketV2` snap-quote mode 3 subscribe, tick pack, ZMQ ipc publish | `smartapi-python`, `pyzmq` |
| **B — Alpha Engine** | 9 calculators (amortized O(1) per tick), EMA smoother, feature score in `[-10, +10]` | `pyzmq`, `deque`-backed windows |
| **C — Broadcaster** | Dedup + rate-limit + Telegram delivery, fully async | `zmq.asyncio`, `redis.asyncio` |

Redis for state (rate counters, cooldowns, mute flag). TimescaleDB for logs (planned).

Per-tick end-to-end latency: **~1 ms** tick-to-score-published, ~200 ms tick-to-Telegram (network dominant).

---

## 9 Calculators (all amortized O(1) per tick)

| # | Function | Role |
|---|----------|------|
| 1 | `_c1_vwap_slope`     | Rolling VWAP slope over 60s (via `WindowFirst`) |
| 2 | `_c2_rvol`           | 60s volume vs 20-min per-minute average (via two `WindowSum`) |
| 3 | `_c3_agg_buy`        | Lee-Ready: LTP vs (bid+ask)/2 → +1 / 0 / -1 |
| 4 | `_c4_sell_absorb`    | Heavy sell qty + flat LTP over 30s → absorption bullish |
| 5 | `_c5_imbalance`      | L1 book (buy_qty − sell_qty)/(buy_qty + sell_qty) × 2 |
| 6 | `_c6_spread`         | Spread filter (> 0.15% → score gated to 0) |
| 7 | `_c7_gap`            | Gap fade in 9:15–9:30 window (planned mode trigger) |
| 8 | `_c8_session_weight` | OPEN_VOL 1.2 · MID_QUIET 0.8 · CLOSE_HOUR 1.1 |
| 9 | EMA smoother         | `alpha = 1 − exp(-dt / tau)`, `tau = 1.0s` (inline in engine loop) |

**Aggregator:**
```
hidden = max(agg, absb) + 0.4 × min(agg, absb)            # same-sign dampen
raw    = session_w × (2.0·vwap + 1.5·hidden + 1.5·imb + 1.0·gap)
score  = clip(raw, -10, +10)  →  EMA smoother
```

---

## Signal Modes

| Mode | Trigger | Cooldown | SL suggestion |
|------|---------|----------|---------------|
| `MOMENTUM`       | `|score| ≥ 8` and `rvol > 2.0` | 300 s | 1.5% |
| `MEAN_REVERSION` | `3 ≤ |score| < 8` and `rvol < 1.5` | 45 s | 0.5% |
| `GAP_FADE`       | 9:15–9:30, `|gap| > 1%` *(planned)* | 180 s | gap-based |

---

## Rate Limits & Kill Switches

**Rate limits:**
- Global: max **20 alerts / hour** (`alpha:count:global:{hour}`)
- Per symbol: max **5 alerts / session** (`alpha:count:{sym}:{YYYYMMDD}`)
- Per (symbol, mode) cooldown via Redis `SET NX EX` (`alpha:cd:{sym}:{mode}`)

**Kill switches:**
1. Market data heartbeat — no tick 5s → suppress alerts *(planned)*
2. Process heartbeat via Redis TTL — miss > 3s → pause *(planned)*
3. Manual mute — Redis flag OR disk file (fail-safe OR-read)
4. Spread filter per-symbol — `> 0.15%` → score gated to 0
5. Alert rate cap (see above)

---

## Quick Start

### 1. Prerequisites

- Ubuntu 24.04 LTS (or any Linux with Python 3.10+)
- Redis 7 running locally
- Angel SmartAPI account with:
  - API key (from smartapi.angelbroking.com)
  - Client code (e.g. `P12345678`)
  - **4-digit MPIN** (not web login password)
  - TOTP secret (base32 string, set during TOTP enrollment)
- Telegram bot token + chat ID (planned)

### 2. Install

```bash
sudo apt update && sudo apt install -y python3-pip python3-venv python3-dev redis-server git
sudo systemctl enable --now redis-server

git clone https://github.com/lieu63081-a11y/alpha_decay_lite_short.git
cd alpha_decay_lite_short

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip wheel
pip install -r requirements.txt
```

### 3. Configure

```bash
cp .env.example .env
nano .env
```

**Required env vars:**

| Var | Value |
|---|---|
| `ANGEL_API_KEY` | SmartAPI key |
| `ANGEL_CLIENT_CODE` | Client ID (e.g. `P12345678`) |
| `ANGEL_PASSWORD` | **4-digit MPIN** (NOT web login password) |
| `ANGEL_TOTP_SECRET` | Base32 secret string (not 6-digit code) |
| `TELEGRAM_TOKEN` | Bot token from `@BotFather` (planned) |
| `TELEGRAM_CHAT_ID` | Your Telegram chat ID (planned) |
| `UNIVERSE` | Comma-separated NSE symbols (e.g. `RELIANCE,TCS,HDFCBANK`) |
| `REDIS_URL` | Default: `redis://localhost:6379/0` |
| `MUTE_FILE` | Default: `/tmp/alpha_mute.flag` |

### 4. Time sync (critical for TOTP)

```bash
sudo timedatectl set-ntp true
timedatectl   # verify "System clock synchronized: yes"
```

### 5. Run

```bash
python alpha_decay_lite.py
```

---

## Expected Output

**Startup:**
```
[B] Alpha Engine up
[C] Broadcaster up
[I 260710 ...] in pool
[A] Logging in to Angel...
[A] Login OK, client=P12345678
[A] Downloading scrip master (~4 MB)...
[A] Scrip master loaded in 12.3s, resolved 5/5 symbols
[A] Data Factory up, 5 tokens mode=3
```

**Live stats (every 10s from B, every 15s from C):**
```
[B] ticks=87 (8/s)  syms=5  top: RELIANCE=+0.42(ltp=1289.5)  TCS=-0.18(ltp=4102.0)  ...
[C] {'scores': 130, 'no_mode': 130, 'muted': 0, 'cooldown': 0, 'rate': 0, 'sent': 0}
```

**On alert (currently stub — prints to stdout):**
```
[TG]
[MEAN_REVERSION] RELIANCE LONG
score=+3.42  peak=3.58  ltp=1289.5
suggested SL: 0.5%
advisory only -- user executes manually
---
```

### Understanding Stats

**Process B:**
- `ticks=N (r/s)` — ticks received in last 10s / rate per second
- `syms=N` — active symbols tracked
- `top:` — top 5 symbols by absolute EMA score, sign shows direction

**Process C counters** (reset every 15s):
- `scores` — total scores received from B
- `no_mode` — score didn't hit mode threshold (score too small, or wrong rvol regime)
- `muted` — mute flag was set
- `cooldown` — same (sym, mode) alerted recently
- `rate` — global/symbol rate limit hit
- `sent` — actual Telegram alerts fired

---

## Operations

### Emergency mute (dual-write, fail-safe OR)

```bash
redis-cli set alpha:mute 1        # Redis path
touch /tmp/alpha_mute.flag        # disk path
```

Either one alone will suppress alerts. To unmute:

```bash
redis-cli del alpha:mute
rm -f /tmp/alpha_mute.flag
```

### Inspect state

```bash
redis-cli keys 'alpha:*'                    # all runtime keys
redis-cli get alpha:count:global:$(date +%s | awk '{print int($1/3600)}')   # global counter
watch -n 2 'redis-cli --scan --pattern "alpha:*" | head -20'
```

### Add symbols to universe

Edit `.env`:
```
UNIVERSE=RELIANCE,TCS,HDFCBANK,INFY,ICICIBANK,ADANIENT,YESBANK,ZOMATO,PAYTM
```

Restart the process. Volatile mid-caps produce far more signal activity than blue chips.

### Run as background service (systemd)

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
ExecStart=/home/alpha/alpha_decay_lite_short/.venv/bin/python /home/alpha/alpha_decay_lite_short/alpha_decay_lite.py
Restart=on-failure
RestartSec=5
StandardOutput=append:/var/log/alpha-decay.log
StandardError=append:/var/log/alpha-decay.log

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now alpha-decay
tail -f /var/log/alpha-decay.log
```

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: requests` (or any) | Deps not installed in active venv | `pip install -r requirements.txt` |
| `ModuleNotFoundError: logzero` | SmartAPI transitive dep — already pinned in requirements.txt | `pip install logzero websocket-client` |
| `Invalid password parameter name` | Using web login password instead of MPIN | Set `ANGEL_PASSWORD=<4-digit-MPIN>` |
| `Invalid Token` / `AB1050` | TOTP time drift | `sudo timedatectl set-ntp true` |
| `[A] FATAL missing env vars: [...]` | `.env` incomplete | Fill all Angel + Telegram fields |
| Silent hang after "in pool" | Scrip master download (~4 MB) in progress | Wait 30–60s (slower on far-region VPS) |
| Ticks not flowing | Market closed (9:15–15:30 IST Mon–Fri) | Wait for market open |
| Scores stuck at 0.0 | All symbols failing spread filter or MID_QUIET blue-chips | Add mid-cap volatile symbols to UNIVERSE |
| `high lat=` warnings | Process B falling behind | Reduce universe size or shard engine |

**Diagnostic: verify env vars loaded (without exposing values):**

```bash
python -c "
from dotenv import load_dotenv, find_dotenv
import os
load_dotenv(find_dotenv())
for k in ['ANGEL_API_KEY','ANGEL_CLIENT_CODE','ANGEL_PASSWORD','ANGEL_TOTP_SECRET']:
    v = os.getenv(k)
    print(f'{k}: {\"SET len=\"+str(len(v)) if v else \"MISSING/EMPTY\"}')"
```

**Reset MPIN (if forgotten):**
Angel One mobile app → Profile → Settings → Reset MPIN → OTP → new 4-digit.

---

## VPS Recommendations

| Scenario | Provider | Cost/month |
|---|---|---|
| Free dev / validation | Oracle Cloud Free Tier (Mumbai, 2 OCPU / 12 GB ARM) | ₹0 |
| Best paid, hourly billing | DigitalOcean Bangalore (4 vCPU / 8 GB) | ~₹4,000 |
| Cheapest paid (Mumbai) | Contabo India (4 vCPU / 8 GB) | ~₹800 |
| Mumbai region + hourly | AWS Lightsail ap-south-1 (4 vCPU / 8 GB) | ~₹3,300 |
| INR billing | E2E Networks Mumbai | ~₹3,000-5,000 |

Signal-only system — no HFT latency budget. 10–20 ms to NSE Mumbai is more than enough (user's manual order-placement latency dominates anyway).

---

## Roadmap

- [x] 3-process pipeline scaffold with real Angel WS integration
- [x] 9 calculators with amortized O(1) rolling windows
- [x] Redis-backed rate limits + cooldowns
- [x] Mute (Redis + disk dual-write, fail-safe OR)
- [x] Spread filter kill switch
- [x] Live stats prints (tick rate, top scores, broadcaster counters)
- [ ] Kill switches #1 (WS 5s heartbeat) + #2 (Redis TTL 3s heartbeat)
- [ ] Telegram bot with ACK/SKIP inline buttons + tracked_position in Redis
- [ ] Exit alerts when ACK'd positions decay back to ±2
- [ ] GAP_FADE mode trigger in `mode_for()`
- [ ] TimescaleDB async logger for tick + score history
- [ ] Scrip master disk cache with mtime check
- [ ] Backtest harness against 3-month historical tick data

---

## Environment

- Python 3.10+ (Ubuntu 24.04 default is 3.12)
- Redis 7+ (AOF everysec recommended)
- Optional: TimescaleDB 2 on PostgreSQL 16 (planned)

## Files

- `alpha_decay_lite.py` — single-file 3-process pipeline (~250 lines)
- `requirements.txt` — Python dependencies (SmartAPI transitive deps pinned)
- `.env.example` — env template for credentials + universe
- `.gitignore` — excludes `.env`, `.venv`, logs, IPC sockets, Redis dumps
