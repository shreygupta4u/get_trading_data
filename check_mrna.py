import sys
sys.path.insert(0, r'C:\Users\mohin\Documents\PythonCodes\get_trading_data\src')
import delta_utils
import pandas as pd
from deltalake import DeltaTable

# 1. What does the snapshot show for MRNA?
snap_path = delta_utils._snapshot_path()
dt = DeltaTable(snap_path)
df = dt.to_pyarrow_dataset().to_table().to_pandas()
df["week_start"] = pd.to_datetime(df["week_start"])
mrna_snap = df[df["symbol"] == "MRNA"][["symbol", "week_start", "Open", "High", "Low", "Close"]]
print("=== MRNA in weekly_snapshot ===")
print(mrna_snap.to_string(index=False))

# 2. What does the weekly table show for MRNA?
print()
print("=== MRNA in weekly table (last 5 rows) ===")
wdf = delta_utils.read_candles(symbols=["MRNA"], period="weekly")
wdf["week_start"] = pd.to_datetime(wdf["week_start"])
print(wdf.sort_values("week_start").tail(5)[["week_start", "Open", "High", "Low", "Close"]].to_string(index=False))

# 3. What path is the snapshot actually at?
print()
print("=== Snapshot path ===")
print(snap_path)

# 4. Are there multiple snapshot locations?
import os
base = r'C:\Users\mohin\Documents\PythonCodes\get_trading_data\data\delta'
print()
print("=== Contents of data/delta/ ===")
for item in sorted(os.listdir(base)):
    full = os.path.join(base, item)
    print(f"  {'[DIR]' if os.path.isdir(full) else '[FILE]'} {item}")
