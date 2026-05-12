import sys
sys.path.insert(0, r'C:\Users\mohin\Documents\PythonCodes\get_trading_data\src')
import yfinance as yf
import pandas as pd
import delta_utils
from deltalake import DeltaTable

for sym in ["COO", "ETSY"]:
    print(f"\n{'='*55}")
    print(f"  {sym}")
    print(f"{'='*55}")

    # yfinance current (last 2 weeks)
    raw = yf.download(sym, start="2026-04-07", auto_adjust=False, progress=False)
    if not raw.empty:
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)
        print(f"yfinance last close : {raw['Close'].iloc[-1]:.2f}")
        print(raw[["Open","High","Low","Close"]].tail(5).to_string())
    else:
        print("yfinance: no data")

    # Snapshot
    snap = DeltaTable(delta_utils._snapshot_path()).to_pyarrow_dataset().to_table().to_pandas()
    row = snap[snap["symbol"] == sym]
    if not row.empty:
        r = row.iloc[0]
        ws = pd.to_datetime(r["week_start"]).date()
        print(f"\nSnapshot : week={ws}  Open={r['Open']:.2f}  High={r['High']:.2f}  Low={r['Low']:.2f}  Close={r['Close']:.2f}  hc={r['High']/r['Close']:.3f}")
    else:
        print("\nSnapshot : NOT FOUND")

    # Weekly table last 5
    wdf = delta_utils.read_candles(symbols=[sym], period="weekly")
    if not wdf.empty:
        wdf["week_start"] = pd.to_datetime(wdf["week_start"])
        print(f"\nWeekly table last 5 rows:")
        print(wdf.sort_values("week_start").tail(5)[["week_start","Open","High","Low","Close"]].to_string(index=False))
    else:
        print("\nWeekly table: no data")
