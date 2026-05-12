import sys
sys.path.insert(0, r'C:\Users\mohin\Documents\PythonCodes\get_trading_data\src')
import json
from pathlib import Path
import config

# How many symbols are reset to 2026-04-11 in the state files?
def load_state(key):
    p = config.SYMBOL_STATE_FILES.get(key)
    if p and p.exists():
        return json.loads(p.read_text())
    return {}

daily_state = load_state("daily")
weekly_state = load_state("weekly")

pending_daily  = [s for s, d in daily_state.items()  if str(d) <= "2026-04-11"]
pending_weekly = [s for s, d in weekly_state.items() if str(d) <= "2026-04-11"]

print(f"Symbols with daily state <= 2026-04-11  : {len(pending_daily)}")
print(f"Symbols with weekly state <= 2026-04-11 : {len(pending_weekly)}")
print(f"Total symbols in daily state            : {len(daily_state)}")
print()

# Test yfinance for 10 of the pending symbols to see if data is clean now
import yfinance as yf
import pandas as pd

test_syms = pending_daily[:10]
print(f"Testing yfinance for 10 pending symbols: {test_syms}")
print()

bad_still = []
for sym in test_syms:
    try:
        df = yf.download(sym, start="2026-04-11", auto_adjust=False, progress=False)
        if df.empty:
            print(f"  {sym}: no data returned")
            continue
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        # Compare Apr 13 close vs Apr 14 close
        df.index = pd.to_datetime(df.index)
        apr13 = df[df.index.date == pd.Timestamp("2026-04-13").date()]
        apr14 = df[df.index.date == pd.Timestamp("2026-04-14").date()]
        if apr13.empty or apr14.empty:
            latest = df["Close"].iloc[-1]
            print(f"  {sym}: missing Apr 13/14 — latest close={latest:.4f}")
            continue
        c13 = float(apr13["Close"].iloc[0])
        c14 = float(apr14["Close"].iloc[0])
        ratio = c14 / c13 if c13 > 0 else 0
        status = "BAD" if ratio < 0.5 else "OK"
        if status == "BAD":
            bad_still.append(sym)
        print(f"  {sym}: Apr13={c13:.2f}  Apr14={c14:.2f}  ratio={ratio:.3f}  [{status}]")
    except Exception as e:
        print(f"  {sym}: ERROR {e}")

print()
print(f"Still bad from sample: {bad_still}")
