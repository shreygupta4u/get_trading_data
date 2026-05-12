import sys
sys.path.insert(0, r'C:\Users\mohin\Documents\PythonCodes\get_trading_data\src')
import pandas as pd
from deltalake import DeltaTable
import delta_utils

base = r'C:\Users\mohin\Documents\PythonCodes\get_trading_data\data\delta'

# Check every Delta table in data/delta/ for MRNA's Close
import os
for folder in sorted(os.listdir(base)):
    path = os.path.join(base, folder)
    if not os.path.isdir(path):
        continue
    try:
        dt = DeltaTable(path)
        df = dt.to_pyarrow_dataset().to_table().to_pandas()
        mrna = df[df["symbol"] == "MRNA"] if "symbol" in df.columns else pd.DataFrame()
        if mrna.empty:
            print(f"[{folder}] rows={len(df)}, MRNA: not found")
        else:
            # find the date column
            dcol = "week_start" if "week_start" in mrna.columns else "date"
            mrna[dcol] = pd.to_datetime(mrna[dcol])
            last = mrna.sort_values(dcol).iloc[-1]
            print(f"[{folder}] rows={len(df)}, MRNA latest: {dcol}={last[dcol].date()}  Close={last['Close']:.4f}")
    except Exception as e:
        print(f"[{folder}] not a valid Delta table or error: {e}")
