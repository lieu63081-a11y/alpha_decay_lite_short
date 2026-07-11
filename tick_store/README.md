# Tick Store

Broker-agnostic NSE tick capture service. One instance per broker on a cheap 1-vCPU / 2 GB VPS. Ticks are stored in daily SQLite files (WAL mode). Query them via a REST API.

**Two-server pattern** — same script on each server, different `BROKER` env var + credentials.

> **Naming note:** every function, variable, and JSON field in `tick_store.py` and this API uses a full, descriptive name (no single-letter or cryptic abbreviations) — e.g. `symbol` not `sym`, `last_traded_price` not `ltp`, `timestamp_ms` not `ts`. This applies to Python identifiers inside the code AND to the field names returned by every API endpoint below.

---

## Cost & Sizing

**Per server:** 1 vCPU, 2 GB RAM, 40-80 GB SSD.

| Provider | Plan | ~₹/month |
|----------|------|----------|
| Contabo India | 4 vCPU / 8 GB / 200 GB *(cheapest even though bigger)* | ~800 |
| Contabo Cloud VPS 10 | 3 vCPU / 8 GB / 75 GB | ~500 |
| Hetzner CPX11 (EU/Helsinki) | 2 vCPU / 2 GB / 40 GB | ~350 |
| Oracle Free Tier (Mumbai) | 2 OCPU / 12 GB (ARM) | **₹0** |

For **two servers**, ₹500-1000/month total (or ₹0 with Oracle × 2 accounts).

**Storage math:** ~500 MB/day/broker (200 symbols × 4 Hz × 6.25h × ~200 bytes). 90-day retention ≈ 45 GB. Fits comfortably on a 60 GB SSD.

---

## Install (each server)

```bash
sudo apt update && sudo apt install -y python3-pip python3-venv git
git clone https://github.com/lieu63081-a11y/alpha_decay_lite_short.git
cd alpha_decay_lite_short/tick_store

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
nano .env                     # fill broker credentials + BROKER=angel or upstox
sudo timedatectl set-ntp true # required for TOTP-based auth

python tick_store.py          # foreground test run
```

Verify: `curl http://localhost:8080/health`

### systemd deployment

```bash
sudo cp tick-store.service /etc/systemd/system/
# Edit paths in the file if your user is not 'ubuntu' or the path differs
sudo systemctl daemon-reload
sudo systemctl enable --now tick-store
sudo systemctl status tick-store
tail -f /var/log/tick-store.log
```

### Firewall (if the API needs remote access)

```bash
sudo ufw allow 8080/tcp
```

**Security:** if exposing publicly, put this behind an Nginx reverse-proxy with Basic Auth, or a Cloudflare Tunnel. Never expose the raw port on the open internet without authentication.

---

## API Endpoints

Base URL: `http://<vps-ip>:8080`. Interactive docs at `/docs`.

### `GET /health`

```json
{
  "ok": true, "broker": "angel",
  "universe": ["RELIANCE", "TCS", "HDFCBANK"],
  "queue_size": 3,
  "stats": {"received": 12034, "written": 12000, "flushes": 12, "errors": 0,
             "last_flush_timestamp": 1720597512.3},
  "data_directory": "./data", "keep_days": 90
}
```

### `GET /dates`
All dates for which stored data exists.

### `GET /symbols?date=YYYYMMDD`
Symbols captured on a given date (default: today).

### `GET /ticks?symbol=RELIANCE&date=20260710&start=09:15&end=10:00&limit=10000`
Raw ticks. Times are IST. `date` defaults to today. `limit` max 100000.

```json
{
  "date": "20260710", "symbol": "RELIANCE", "count": 1234,
  "ticks": [
    {"timestamp_ms": 1720597500123, "last_traded_price": 1289.5,
     "best_bid_price": 1289.4, "best_ask_price": 1289.6,
     "cumulative_day_volume": 2140000, "daily_vwap": 1287.3,
     "total_buy_quantity": 45000, "total_sell_quantity": 42000},
    ...
  ]
}
```

### `GET /latest?symbol=RELIANCE&count=50`
Last N ticks for today.

### `GET /ohlc?symbol=RELIANCE&date=20260710&interval=1m`
OHLC candles built from stored ticks. `interval` e.g. `30s`, `1m`, `5m`, `15m`.

```json
{
  "date": "20260710", "symbol": "RELIANCE", "interval": "1m",
  "candles": [
    {"timestamp_ms": 1720597500000, "open": 1289.5, "high": 1290.1,
     "low": 1289.3, "close": 1289.8, "volume": 12000},
    ...
  ]
}
```

---

## Adding a Second Broker

`tick_store.py` has an `upstox_ingester()` function with a full template docstring showing exactly which fields to populate. To use it:

1. `pip install upstox-python-sdk` (or your broker's SDK)
2. Fill in the commented-out section inside `upstox_ingester()` per that SDK's docs
3. Add credentials to `.env`
4. Set `BROKER=upstox`

Any broker that provides a WebSocket + Python SDK can be adapted in roughly 30 lines. Just push a dict shaped like the one `angel_ingester()` builds into `tick_queue.put({...})` — the writer thread handles persistence from there, using the exact same field names (`symbol`, `timestamp`, `last_traded_price`, `best_bid_price`, `best_ask_price`, `cumulative_day_volume`, `daily_vwap`, `total_buy_quantity`, `total_sell_quantity`, `open_price`, `previous_close_price`).

Broker options (all with a Python SDK + WebSocket):
- **Angel One** — free, well-documented (fully implemented)
- **Upstox** — free, v2 SDK (`upstox-python-sdk`)
- **Zerodha Kite** — ₹2000/month, `kiteconnect`
- **Dhan** — free, `dhanhq`
- **Fyers** — free, `fyers-apiv3`
- **5paisa** — free, `py5paisa`

---

## Ops

### Check disk usage
```bash
du -sh data/
ls -lh data/ | tail -20
```

### Manual cleanup (before the scheduled hourly pass)
```bash
find data/ -name 'ticks_*.db*' -mtime +30 -delete
```

### Backup off-server
```bash
rsync -az data/ backup-server:/backups/tickstore-broker-$(hostname)/
```

### Query from a remote machine
```bash
curl "http://<vps-ip>:8080/ticks?symbol=RELIANCE&date=20260710&start=09:15&end=09:30&limit=50" \
  | jq '.ticks | length'
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `[angel] FATAL login` | Wrong MPIN or TOTP secret | Verify `.env` per the Angel setup guide |
| `queue_size` growing steadily | Writer thread stuck (rare) | `systemctl restart tick-store` |
| `errors` counter growing | SQLite disk write failures | Check `df -h` |
| API returns empty for today's data | Ingester not running, or market closed | `curl /health` — check `stats.received` |
| CPU pegged during market open | Batch flush blocking too long | Increase `FLUSH_INTERVAL_S`, decrease `FLUSH_BATCH_MAX` |

---

## Architecture

```
Broker WebSocket  ─→  ingester thread  ─→  Queue  ─→  writer thread  ─→  SQLite (WAL)
                                                                              │
                                                                       cleanup thread
                                                                       (deletes >90 days old)
                                                                              │
FastAPI async endpoints  ─read-only→  SQLite files
                        ↑
                     external client (curl / Python / anything)
```

Single Python process. Three background threads (`writer_loop`, `cleanup_loop`, and the broker-specific ingester function) plus the FastAPI event loop. All communicate via the module-level `tick_queue` (a standard `queue.Queue`). SQLite runs in WAL mode, which allows concurrent read-only connections (used by the API endpoints) while the writer thread is inserting.

---

## Notes

- The storage schema is intentionally denormalized (`symbol` as a plain TEXT column, not a foreign key) for simplicity and speed.
- Timestamps are stored as `timestamp_ms` (INTEGER milliseconds since epoch) — SQLite handles this natively, no timezone conversion needed at query time.
- Daily file rotation happens automatically inside `writer_loop`, triggered by the first tick whose timestamp falls on a new IST calendar day.
- The service is **stateless beyond the SQLite files** — safe to restart at any time; at most the last few seconds of buffered-but-not-yet-flushed ticks are lost.

## Files

- `tick_store.py` — the service itself (ingesters + writer + cleanup + FastAPI), single file
- `requirements.txt` — Python dependencies
- `.env.example` — environment variable template (broker selection, credentials, storage/API config)
- `tick-store.service` — systemd unit file for production deployment
