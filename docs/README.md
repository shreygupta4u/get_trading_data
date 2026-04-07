# NASDAQ Daily Candle Fetcher

Fetches every NASDAQ-listed equity's daily OHLCV candle and stores it locally
in date-partitioned **Parquet** format. A scheduler handles daily automation
with automatic catch-up after downtime.

---

## Project Structure

```
nasdaq_candles/
├── config.py                  ← All tuneable settings live here
├── fetch_nasdaq_candles.py    ← Core fetch + storage logic
├── scheduler.py               ← APScheduler wrapper + catch-up logic
├── requirements.txt           ← Python dependencies
├── logs/
│   ├── fetch.log              ← Fetch activity (auto-rotated)
│   └── scheduler.log          ← Scheduler activity (auto-rotated)
└── data/
    └── year=YYYY/
        └── month=MM/
            └── YYYYMMDD.parquet   ← One file per trading day
```

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. (Optional) Edit config.py to change schedule time, batch size, etc.

# 3. Run a one-off fetch for today
python fetch_nasdaq_candles.py

# 4. Back-fill a date range
python fetch_nasdaq_candles.py --date 2024-01-01 --date 2024-12-31

# 5. Start the persistent scheduler (blocks; use nohup / service for background)
python scheduler.py
```

---

## Scheduler Options

| Flag | Effect |
|---|---|
| *(none)* | Run catch-up on startup, then schedule daily at configured time |
| `--catchup-only` | Run catch-up and exit (good for cron-managed environments) |
| `--run-now` | Fetch today immediately, then continue scheduling |

---

## Reading the Data

```python
import pandas as pd

# Single day
df = pd.read_parquet("data/year=2024/month=12/20241231.parquet")

# All data for a year (Hive-style partition discovery)
df = pd.read_parquet("data/year=2024/")

# Filter to a specific symbol across all dates
import pyarrow.dataset as ds
dataset = ds.dataset("data/", format="parquet", partitioning="hive")
df = dataset.to_table(filter=ds.field("symbol") == "AAPL").to_pandas()
```

---

## Running as a Background Service

### Linux — systemd

Create `/etc/systemd/system/nasdaq-candles.service`:

```ini
[Unit]
Description=NASDAQ Daily Candle Fetcher
After=network.target

[Service]
Type=simple
User=YOUR_USERNAME
WorkingDirectory=/path/to/nasdaq_candles
ExecStart=/usr/bin/python3 /path/to/nasdaq_candles/scheduler.py
Restart=always
RestartSec=60

[Install]
WantedBy=multi-user.target
```

Then:
```bash
sudo systemctl daemon-reload
sudo systemctl enable nasdaq-candles
sudo systemctl start nasdaq-candles
sudo systemctl status nasdaq-candles
```

### macOS — launchd

Create `~/Library/LaunchAgents/com.nasdaq.candles.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
    "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>              <string>com.nasdaq.candles</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>/path/to/nasdaq_candles/scheduler.py</string>
    </array>
    <key>RunAtLoad</key>          <true/>
    <key>KeepAlive</key>          <true/>
    <key>WorkingDirectory</key>   <string>/path/to/nasdaq_candles</string>
    <key>StandardOutPath</key>    <string>/path/to/nasdaq_candles/logs/launchd.log</string>
    <key>StandardErrorPath</key>  <string>/path/to/nasdaq_candles/logs/launchd_err.log</string>
</dict>
</plist>
```

```bash
launchctl load ~/Library/LaunchAgents/com.nasdaq.candles.plist
```

### Windows — Task Scheduler (PowerShell)

```powershell
$action  = New-ScheduledTaskAction `
    -Execute "python.exe" `
    -Argument "C:\path\to\nasdaq_candles\scheduler.py" `
    -WorkingDirectory "C:\path\to\nasdaq_candles"

# Trigger 1: Daily at 4:15 PM
$daily   = New-ScheduledTaskTrigger -Daily -At "4:15PM"

# Trigger 2: At machine startup (catch-up runs automatically via scheduler.py)
$startup = New-ScheduledTaskTrigger -AtStartup

$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 3) `
    -MultipleInstances IgnoreNew

Register-ScheduledTask `
    -TaskName "NASDAQ Candle Fetcher" `
    -Action   $action `
    -Trigger  $daily, $startup `
    -Settings $settings `
    -RunLevel Highest
```

---

## Configuration Reference

Edit **config.py** to change any of these:

| Setting | Default | Description |
|---|---|---|
| `DATA_DIR` | `./data` | Root folder for parquet files |
| `SCHEDULE_HOUR` | `16` | Hour of daily run (24h local) |
| `SCHEDULE_MINUTE` | `15` | Minute of daily run |
| `CATCHUP_DAYS` | `7` | Look-back window for missed days |
| `BATCH_SIZE` | `100` | Symbols per yfinance batch |
| `BATCH_DELAY_SECONDS` | `2` | Pause between batches |
| `RETRY_ATTEMPTS` | `3` | Retries per failed batch |
| `PARQUET_COMPRESSION` | `snappy` | Parquet codec |
| `COLUMNS_TO_KEEP` | OHLCV | Columns written per symbol |

---

## Notes

- **Market holidays** are not automatically skipped. The fetcher will attempt
  a run, receive no data, log a warning, and write nothing — this is harmless.
- yfinance data comes from Yahoo Finance and is suitable for personal/research
  use. For production trading systems, use a licensed data vendor.
- Symbol list is sourced from the official NASDAQ FTP directory and refreshed
  on every run (~3 500 symbols as of 2025).
