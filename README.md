# Alpha-Decay Lite

Signal-only NSE cash equity alerting engine. Advisory only -- no order placement.

Angel One SmartAPI se live market data lo, 60-second sliding window par score compute karo,
high-quality Buy/Sell alerts Telegram par bhejo. User manually order place karta hai broker app par.

## Architecture (3 processes)

```
Angel WS (mode 3)  ->  Process A  --ZMQ ticks-->   Process B  --ZMQ scores-->  Process C  ->  Telegram
                       Data Factory                Alpha Engine                 Broadcaster
                                                   9 calc + EMA smoother        dedup + rate-limit + async Redis
```

- **Process A -- Data Factory:** Angel `SmartWebSocketV2` snap-quote mode 3, ZMQ ipc publish.
- **Process B -- Alpha Engine:** 9 calculators, time-weighted EMA, feature score in [-10, +10].
- **Process C -- Signal Broadcaster:** async, dedup, rate-limit, deliver to Telegram.
- Redis for state, TimescaleDB for logs (planned).

## 9 Calculators (all amortized O(1) per tick)

| # | Function | Role |
|---|----------|------|
| 1 | `_c1_vwap_slope`     | Rolling VWAP + slope over 60s |
| 2 | `_c2_rvol`           | 60s volume vs 20-min per-minute avg |
| 3 | `_c3_agg_buy`        | Lee-Ready aggressive buy classification |
| 4 | `_c4_sell_absorb`    | Heavy sell qty + flat LTP -> absorption |
| 5 | `_c5_imbalance`      | Bid-Ask L1 imbalance |
| 6 | `_c6_spread`         | Spread filter (>0.15% gates score to 0) |
| 7 | `_c7_gap`            | Gap fade in 9:15-9:30 window |
| 8 | `_c8_session_weight` | OPEN_VOL / MID_QUIET / CLOSE_HOUR weighting |
| 9 | EMA smoother         | `alpha = 1 - exp(-dt/tau)`, tau = 1.0s (inline) |

## Signal Modes

| Mode | Trigger | Cooldown |
|------|---------|----------|
| `MOMENTUM`       | `|score| >= 8` and `rvol > 2.0` | 300 s |
| `MEAN_REVERSION` | `3 <= |score| < 8` and `rvol < 1.5` | 45 s |
| `GAP_FADE`       | 9:15-9:30, `|gap| > 1%` (planned) | 180 s |

## Rate Limits

- Global: max **20 alerts / hour**
- Per symbol: max **5 alerts / session**
- Cooldown per (symbol, mode) via Redis `SET NX EX`

## Kill Switches

1. Market data heartbeat (planned) -- no tick 5s -> suppress alerts
2. Process heartbeat via Redis TTL (planned)
3. Manual mute -- Redis flag OR disk file (fail-safe OR-read)
4. Spread filter per-symbol (`> 0.15%` -> score = 0)
5. Alert rate cap (see above)

## Quick Start

```bash
# 1. install deps
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. configure
cp .env.example .env
nano .env  # fill Angel + Telegram creds

# 3. run Redis locally
redis-server &

# 4. launch
python alpha_decay_lite.py
```

## Environment

Python 3.10+ (Ubuntu 24.04 default is 3.12). See `.env.example` for required credentials.

## Design Contract

Yeh system kabhi order fire nahi karta. Sirf actionable signals Telegram par deliver karta hai.
Saara execution risk aur final trade decision user ke paas hai.
**Algo advisory hai, algo execution nahi.**
