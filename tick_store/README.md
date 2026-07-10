# Tick Store

Broker-agnostic NSE tick capture service. One instance per broker on cheap 1-vCPU / 2 GB VPS. Ticks stored in daily SQLite files (WAL mode). Query via REST API.

**Two-server pattern** — same script on each, different `BROKER` env var + credentials.

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

**Storage math:** ~500 MB/day/broker (200 symbols × 4 Hz × 6.25h × ~200 bytes). 90-day retention ≈ 45 GB. Fits comfortably on 60 GB SSD.

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
nano .env                     # fill broker creds + BROKER=angel or upstox
sudo timedatectl set-ntp true # required for TOTP-based auth

python tick_store.py          # foreground test run
```

Verify: `curl http://localhost:8080/health`

### systemd deployment

```bash
sudo cp tick-store.service /etc/systemd/system/
# Edit paths in the file if your user is not 'ubuntu' or path differs
sudo systemctl daemon-reload
sudo systemctl enable --now tick-store
sudo systemctl status tick-store
tail -f /var/log/tick-store.log
```

### Firewall (if API needs remote access)

```bash
sudo ufw allow 8080/tcp
```

**Security:** if exposing publicly, put behind Nginx reverse-proxy with Basic Auth or Cloudflare Tunnel. Never expose the raw port on the open internet without auth.

---

## API Endpoints

Base URL: `http://<vps-ip>:8080`. Interactive docs at `/docs`.

### `GET /health`

```json
{
  "ok": true, "broker": "angel", "queue_size": 3,
  "stats": {"received": 12034, "written": 12000, "flushes": 12, "errors": 0},
  "data_dir": "./data", "keep_days": 90
}
```

### `GET /dates`
All dates for which we have stored data.

### `GET /symbols?date=YYYYMMDD`
Symbols captured on a given date (default: today).

### `GET /ticks?sym=RELIANCE&date=20260710&start=09:15&end=10:00&limit=10000`
Raw ticks. Times are IST. `date` defaults today. `limit` max 100000.

```json
{
  "date": "20260710", "sym": "RELIANCE", "count": 1234,
  "ticks": [
    {"ts": 1720597500123, "ltp": 1289.5, "bid": 1289.4, "ask": 1289.6,
     "vol": 2140000, "vwap": 1287.3, "buy_qty": 45000, "sell_qty": 42000},
    ...
  ]
}
```

### `GET /latest?sym=RELIANCE&n=50`
Last N ticks for today.

### `GET /ohlc?sym=RELIANCE&date=20260710&interval=1m`
OHLC candles from stored ticks. `interval` e.g. `30s`, `1m`, `5m`, `15m`.

```json
{
  "date": "20260710", "sym": "RELIANCE", "interval": "1m",
  "candles": [
    {"ts": 1720597500000, "o": 1289.5, "h": 1290.1, "l": 1289.3, "c": 1289.8, "v": 12000},
    ...
  ]
}
```

---

## Adding a Second Broker

`tick_store.py` has an `upstox_ingester()` template with all needed hooks. To use:

1. `pip install upstox-python-sdk` (or your broker's SDK)
2. Fill in the commented section in `upstox_ingester()` per SDK docs
3. Add credentials to `.env`
4. Set `BROKER=upstox`

Any broker that provides a WebSocket + Python SDK can be adapted in ~30 lines. Just push messages into `q_ticks.put({...})` in the tick dict schema — writer handles the rest.

Broker options (all with Python SDK + WebSocket):
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

### Manual cleanup (before scheduled)
```bash
find data/ -name 'ticks_*.db*' -mtime +30 -delete
```

### Backup off-server
```bash
rsync -az data/ backup-server:/backups/tickstore-broker-$(hostname)/
```

### Query from remote machine
```bash
curl "http://<vps-ip>:8080/ticks?sym=RELIANCE&date=20260710&start=09:15&end=09:30&limit=50" \
  | jq '.ticks | length'
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `[angel] FATAL login` | Wrong MPIN or TOTP secret | Verify `.env` per Angel guide |
| `queue_size` growing steadily | Writer thread stuck (rare) | `systemctl restart tick-store` |
| `errors` counter growing | SQLite disk write failures | Check `df -h` |
| API returns empty for today's data | Ingester not running or market closed | `curl /health` — check `stats.received` |
| CPU pegged during market open | Batch flush blocking too long | Increase `FLUSH_INTERVAL_S`, decrease `FLUSH_BATCH_MAX` |

---

## Architecture

```
Broker WebSocket  ─→  ingester thread  ─→  Queue  ─→  writer thread  ─→  SQLite (WAL)
                                                                              ↓
                                                                       cleanup thread
                                                                       (deletes >90d)
                                                                              ↓
FastAPI async endpoints  ─read-only→  SQLite files
                        ↑
                     external client (curl / Python / anything)
```

Single Python process. Three background threads (writer, cleanup, ingester) + FastAPI event loop. All communicate via `queue.Queue`. SQLite in WAL mode allows concurrent reads while the writer thread is inserting.

---

## Notes

- Storage schema is intentionally denormalized (`sym` as TEXT column, not FK) for simplicity and speed.
- Timestamps stored as INTEGER ms (SQLite handles this natively, no timezone).
- Daily file rotation happens on the writer thread's first tick past midnight IST.
- The service is **stateless beyond the SQLite files** — safe to restart anytime; only lose the last <5s of buffered ticks.
