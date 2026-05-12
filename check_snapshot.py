import sys
sys.path.insert(0, r'C:\Users\mohin\Documents\PythonCodes\get_trading_data\src')
import delta_utils
import pandas as pd
from deltalake import DeltaTable

# Check snapshot table
snap_path = delta_utils._snapshot_path()
print("Snapshot path:", snap_path)

dt = DeltaTable(snap_path)
df = dt.to_pyarrow_dataset().to_table().to_pandas()
df["week_start"] = pd.to_datetime(df["week_start"])

print(f"Snapshot rows: {len(df)}, latest week_start: {df['week_start'].max().date()}")
print()

syms = ["JPM", "HD", "MSFT", "NVDA", "AAPL", "GOOGL"]
check = df[df["symbol"].isin(syms)][["symbol", "week_start", "Close"]].sort_values("symbol")
print("Current snapshot values:")
print(check.to_string(index=False))
print()

# Check weekly table directly
wdf = delta_utils.read_candles(symbols=["JPM", "AAPL"], period="weekly")
wdf["week_start"] = pd.to_datetime(wdf["week_start"])
print("Weekly table - last 3 rows per symbol:")
print(wdf.groupby("symbol").tail(3)[["symbol", "week_start", "Close"]].to_string(index=False))
