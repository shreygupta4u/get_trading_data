import sys
sys.path.insert(0, r'C:\Users\mohin\Documents\PythonCodes\get_trading_data\src')
import yfinance as yf
import pandas as pd
import delta_utils
from deltalake import DeltaTable

syms = ["COO", "ESTY"]

for sym in syms:
    print(f"{'='*50}")
    print(f"  {sym}")
    print(f"{'='*50}")

    # 1. yfinance current price (last 2 weeks)
    df = yf.download(sym, start="2026-04-07", auto_adjust=False, progress=False)
    if not df.empty:
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        print(f"yfinance (Apr 7+):  last close = {df['Close'].iloc[-1]:.4f}")
        print(df[["Open","High","Low","Close"]].tail(5).to_string())
    else:
        print(f"yfinance: no data")

    print()

    # 2. Weekly snapshot
    snap = DeltaTable(delta_utils._snapshot_path()).to_pyarrow_dataset().to_table().to_pandas()
    row = snap[snap["symbol"] == sym]
    if not row.empty:
        r = row.iloc[0]
        print(f"Snapshot:  week_start={pd.to_datetime(r['week_start']).date()}  Open={r['Open']:.4f}  High={r['High']:.4f}  Low={r['Low']:.4f}  Close={r['Close']:.4f}")
    else:
        print(f"Snapshot:  not found")

    print()

    # 3. Weekly table last 5 rows
    wdf = delta_utils.read_candles(symbols=[sym], period="weekly")
    if not wdf.empty:
        wdf["week_start"] = pd.to_datetime(wdf["week_start"])
        print(f"Weekly table (last 5):")
        print(wdf.sort_values("week_start").tail(5)[["week_start","Open","High","Low","Close"]].to_string(index=False))
    else:
        print(f"Weekly table: no data")

    print()

    # 4. Daily table last 5 rows
    ddf = delta_utils.read_candles(symbols=[sym], period="daily", start="2026-04-07")
    if not ddf.empty:
        ddf["date"] = pd.to_datetime(ddf["date"])
        print(f"Daily table (Apr 7+):")
        print(ddf.sort_values("date")[["date","Open","High","Low","Close"]].to_string(index=False))
    else:
        print(f"Daily table: no data for Apr 7+")

    print()
