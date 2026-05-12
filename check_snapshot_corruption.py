import sys
sys.path.insert(0, r'C:\Users\mohin\Documents\PythonCodes\get_trading_data\src')
import pandas as pd
from deltalake import DeltaTable
import delta_utils

# Load the full snapshot
snap_path = delta_utils._snapshot_path()
dt = DeltaTable(snap_path)
df = dt.to_pyarrow_dataset().to_table().to_pandas()
df["week_start"] = pd.to_datetime(df["week_start"])

print(f"Snapshot: {len(df)} rows, week_start range: {df['week_start'].min().date()} to {df['week_start'].max().date()}")
print()

# Detect corrupted rows: High/Close > 1.08 (same logic as fix_bad_prices)
df["hc_ratio"] = df["High"] / df["Close"].replace(0, float("nan"))
bad = df[df["hc_ratio"] > 1.08].copy()
print(f"Rows with High/Close > 1.08 (suspected corrupt): {len(bad)}")
if not bad.empty:
    print()
    print("Sample of worst rows:")
    print(bad.nlargest(20, "hc_ratio")[["symbol", "week_start", "Open", "High", "Low", "Close", "hc_ratio"]].to_string(index=False))

print()
# Also check for unrealistically low Close (< $0.50 for stocks that had High > $5)
sus_low = df[(df["Close"] < 0.50) & (df["High"] > 5.0)]
print(f"Rows with Close < $0.50 but High > $5.00 (extreme corruption): {len(sus_low)}")
if not sus_low.empty:
    print(sus_low[["symbol", "week_start", "High", "Close"]].head(20).to_string(index=False))

print()
# Distribution of week_start dates in snapshot
print("Week_start distribution in snapshot:")
print(df["week_start"].value_counts().sort_index().to_string())
