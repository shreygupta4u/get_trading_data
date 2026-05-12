import sys
sys.path.insert(0, r'C:\Users\mohin\Documents\PythonCodes\get_trading_data\src')
import yfinance as yf
import pandas as pd
import delta_utils

print("=== yfinance raw fetch for MRNA (Apr 11 onwards) ===")
df = yf.download("MRNA", start="2026-04-11", auto_adjust=False, progress=False)
if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.get_level_values(0)
print(df[["Open", "High", "Low", "Close", "Volume"]].to_string())

print()
print("=== Daily table - MRNA last 10 rows ===")
ddf = delta_utils.read_candles(symbols=["MRNA"], period="daily")
ddf["date"] = pd.to_datetime(ddf["date"])
print(ddf.sort_values("date").tail(10)[["date", "Open", "High", "Low", "Close", "Volume"]].to_string(index=False))

print()
print("=== Weekly table - MRNA last 5 rows ===")
wdf = delta_utils.read_candles(symbols=["MRNA"], period="weekly")
wdf["week_start"] = pd.to_datetime(wdf["week_start"])
print(wdf.sort_values("week_start").tail(5)[["week_start", "Open", "High", "Low", "Close"]].to_string(index=False))
