import sys
sys.path.insert(0, r'C:\Users\mohin\Documents\PythonCodes\get_trading_data\src')
import delta_utils
import pandas as pd

# Check the daily table for key symbols around the bad dates
syms = ["JPM", "HD", "MSFT", "NVDA", "AAPL", "GOOGL"]
df = delta_utils.read_candles(symbols=syms, period="daily", start="2026-04-10")
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["symbol", "date"])

print("Daily table Apr 10+ for key symbols:")
print(df[["symbol", "date", "Open", "High", "Low", "Close"]].to_string(index=False))
print()

# Also check what the weekly table has for April 13 week
wdf = delta_utils.read_candles(symbols=syms, period="weekly", start="2026-04-06")
wdf["week_start"] = pd.to_datetime(wdf["week_start"])
print("Weekly table Apr 6+ for key symbols:")
print(wdf[["symbol", "week_start", "Open", "High", "Low", "Close"]].to_string(index=False))
