import yfinance as yf
import pandas as pd

for sym in ["JPM", "HD", "AAPL", "MSFT", "NVDA"]:
    print(f"--- {sym} ---")
    for adj in [False, True]:
        try:
            df = yf.download(sym, start="2026-04-11", auto_adjust=adj, progress=False)
            if df.empty:
                print(f"  auto_adjust={adj}: empty")
                continue
            # flatten MultiIndex if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            last = df[["Open", "Close"]].tail(4)
            print(f"  auto_adjust={adj}:")
            for idx, row in last.iterrows():
                print(f"    {idx.date()}  Open={row['Open']:.2f}  Close={row['Close']:.2f}")
        except Exception as e:
            print(f"  auto_adjust={adj}: ERROR {e}")
    print()
