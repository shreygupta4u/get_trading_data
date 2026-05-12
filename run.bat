@echo off
title FVB33 Data Pipeline

set PYTHON=C:\Users\mohin\AppData\Local\Python\pythoncore-3.14-64\python.exe
set DIR=C:\Users\mohin\Documents\PythonCodes\get_trading_data

cd /d "%DIR%\src"
if not exist "..\logs" mkdir "..\logs"

echo.
echo ============================================================
echo  FVB33 Data Pipeline
echo  %DATE%  %TIME%
echo ============================================================
echo.
echo  --- Normal runs ---
echo    1. Quick run        (daily prices + weekly indicators + signals)
echo    2. Full run         (daily prices + DAILY indicators + weekly + signals)
echo    3. Daily prices only             (no indicators, no signals)
echo    4. Weekly incremental            (weekly indicators + signals + snapshot)
echo    5. Daily indicators only         (recompute daily technicals)
echo.
echo  --- Force recompute ---
echo    6. Force full re-fetch           (re-fetch everything from scratch)
echo    7. Force weekly indicators only  (fix indicator logic changes)
echo    8. Force signals only            (fix signal logic changes)
echo    9. Force weekly indicators + signals  (USE AFTER ANY LOGIC CHANGE)
echo.
echo  --- Portfolio backtest (S&P 500 fund simulation) ---
echo    E. Portfolio backtest — incremental  (process new weeks only)
echo    F. Portfolio backtest — FORCE full recompute
echo.
echo  --- Market backtest (full market — all delta symbols) ---
echo    G. Market backtest 60 — incremental  (process new weeks only)
echo    H. Market backtest 60 — FORCE full recompute
echo    I. Market backtest 50 — incremental
echo    J. Market backtest 50 — FORCE full recompute
echo    K. Market backtest 40 — incremental
echo    L. Market backtest 40 — FORCE full recompute
echo    M. Market backtest 30 — incremental
echo    N. Market backtest 30 — FORCE full recompute
echo.
echo  --- Scoring ---
echo    O. Compute scores        (incremental — RS + RVR + Band Position)
echo    P. Compute scores FORCE  (recompute all weeks from scratch)
echo.
echo  --- Sector ETF ---
echo    Q. Fetch sector ETF data  (incremental — 11 SPDR ETFs + SPY)
echo    R. Fetch sector ETF FORCE (re-fetch full history from scratch)
echo.
echo  --- Market Cap ---
echo    S. Fetch market caps      (incremental — skip if fetched in last 30 days)
echo    T. Fetch market caps FORCE (re-fetch all 6700 symbols)
echo.
echo  --- SNP backtest (S&P 500 only — no penny stocks) ---
echo    U. SNP backtest 60 — incremental
echo    V. SNP backtest 60 — FORCE full recompute
echo    W. SNP backtest 50 — incremental
echo    X. SNP backtest 50 — FORCE full recompute
echo    Y. SNP backtest 40 — incremental
echo    Z. SNP backtest 40 — FORCE full recompute
echo    1U. SNP backtest 30 — incremental
echo    1V. SNP backtest 30 — FORCE full recompute
echo.
echo  --- SNP Trade Export (Excel) ---
echo    2A. Export SNP trades cap 60  (all strategies to Excel)
echo    2B. Export SNP trades cap 50
echo    2C. Export SNP trades cap 40
echo    2D. Export SNP trades cap 30
echo.
echo  --- Maintenance ---
echo    A. Vacuum tables         (reclaim disk space - run monthly)
echo    B. Rebuild snapshot      (rebuild scanner table from current data)
echo    C. Fix bad prices        (detect + remove corruption, then re-run option 1)
echo    D. Fix bad prices ALL    (nuclear: wipe ALL data from Apr 13+)
echo.
echo    0. Exit
echo.
set /p CHOICE="Enter choice [0-9 / A-Z / 1U-1V / 2A-2D]: "

if "%CHOICE%"=="1" goto RUN_QUICK
if "%CHOICE%"=="2" goto RUN_FULL
if "%CHOICE%"=="3" goto RUN_DAILY_PRICES
if "%CHOICE%"=="4" goto RUN_WEEKLY
if "%CHOICE%"=="5" goto RUN_DAILY_INDICATORS
if "%CHOICE%"=="6" goto RUN_FORCE
if "%CHOICE%"=="7" goto RUN_FORCE_INDICATORS
if "%CHOICE%"=="8" goto RUN_FORCE_SIGNALS
if "%CHOICE%"=="9" goto RUN_FORCE_INDICATORS_SIGNALS
if /i "%CHOICE%"=="E" goto RUN_PORTFOLIO
if /i "%CHOICE%"=="F" goto RUN_PORTFOLIO_FORCE
if /i "%CHOICE%"=="G" goto RUN_MARKET
if /i "%CHOICE%"=="H" goto RUN_MARKET_FORCE
if /i "%CHOICE%"=="I" goto RUN_MARKET50
if /i "%CHOICE%"=="J" goto RUN_MARKET50_FORCE
if /i "%CHOICE%"=="K" goto RUN_MARKET40
if /i "%CHOICE%"=="L" goto RUN_MARKET40_FORCE
if /i "%CHOICE%"=="M" goto RUN_MARKET30
if /i "%CHOICE%"=="N" goto RUN_MARKET30_FORCE
if /i "%CHOICE%"=="O" goto RUN_SCORES
if /i "%CHOICE%"=="P" goto RUN_SCORES_FORCE
if /i "%CHOICE%"=="Q" goto RUN_ETF
if /i "%CHOICE%"=="R" goto RUN_ETF_FORCE
if /i "%CHOICE%"=="S" goto RUN_CAPS
if /i "%CHOICE%"=="T" goto RUN_CAPS_FORCE
if /i "%CHOICE%"=="U" goto RUN_SNP60
if /i "%CHOICE%"=="V" goto RUN_SNP60_FORCE
if /i "%CHOICE%"=="W" goto RUN_SNP50
if /i "%CHOICE%"=="X" goto RUN_SNP50_FORCE
if /i "%CHOICE%"=="Y" goto RUN_SNP40
if /i "%CHOICE%"=="Z" goto RUN_SNP40_FORCE
if /i "%CHOICE%"=="1U" goto RUN_SNP30
if /i "%CHOICE%"=="1V" goto RUN_SNP30_FORCE
if /i "%CHOICE%"=="2A" goto RUN_SNP_EXPORT60
if /i "%CHOICE%"=="2B" goto RUN_SNP_EXPORT50
if /i "%CHOICE%"=="2C" goto RUN_SNP_EXPORT40
if /i "%CHOICE%"=="2D" goto RUN_SNP_EXPORT30
if /i "%CHOICE%"=="A" goto RUN_VACUUM
if /i "%CHOICE%"=="B" goto RUN_SNAPSHOT
if /i "%CHOICE%"=="C" goto RUN_FIX_PRICES
if /i "%CHOICE%"=="D" goto RUN_FIX_ALL
if "%CHOICE%"=="0" goto END

echo Invalid choice.
goto END

:RUN_QUICK
echo.
echo [%TIME%] Quick run: daily prices + weekly indicators + signals...
echo.
"%PYTHON%" orchestrator.py --no-daily-technicals
goto DONE

:RUN_FULL
echo.
echo [%TIME%] Full run: daily prices + daily indicators + weekly + signals...
echo.
"%PYTHON%" orchestrator.py
goto DONE

:RUN_DAILY_PRICES
echo.
echo [%TIME%] Fetching daily prices only (no indicators)...
echo.
"%PYTHON%" orchestrator.py --period daily --no-daily-technicals
goto DONE

:RUN_WEEKLY
echo.
echo [%TIME%] Weekly incremental: indicators + signals + snapshot...
echo.
"%PYTHON%" orchestrator.py --period weekly
goto DONE

:RUN_DAILY_INDICATORS
echo.
echo [%TIME%] Computing daily indicators only (no fetch)...
echo.
"%PYTHON%" technical_analysis.py --period daily
goto DONE

:RUN_FORCE
echo.
echo [%TIME%] FORCE full re-fetch + recompute everything...
echo.
"%PYTHON%" orchestrator.py --force
goto DONE

:RUN_FORCE_INDICATORS
echo.
echo [%TIME%] FORCE weekly indicators recompute (all 6700 symbols)...
echo  Use this after any indicator logic change (FVB, BX, etc.)
echo.
"%PYTHON%" technical_analysis.py --period weekly --force
goto DONE

:RUN_FORCE_SIGNALS
echo.
echo [%TIME%] FORCE signals recompute (all 6700 symbols)...
echo  Use this after any signal logic change in signals.py
echo.
"%PYTHON%" signals.py --force
goto DONE

:RUN_FORCE_INDICATORS_SIGNALS
echo.
echo [%TIME%] FORCE weekly indicators + signals recompute (all 6700 symbols)...
echo  USE THIS after any change to indicator or signal logic.
echo  Step 1/2: Recomputing weekly indicators...
echo.
"%PYTHON%" technical_analysis.py --period weekly --force
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Indicators failed. Stopping before signals.
    goto DONE
)
echo.
echo  Step 2/2: Recomputing signals...
echo.
"%PYTHON%" signals.py --force
goto DONE

:RUN_PORTFOLIO
echo.
echo [%TIME%] Portfolio backtest — incremental (S&P 500 fund simulation)...
echo.
"%PYTHON%" portfolio_backtest.py
goto DONE

:RUN_PORTFOLIO_FORCE
echo.
echo [%TIME%] Portfolio backtest — FORCE full recompute...
echo  (Re-runs entire history from scratch)
echo.
"%PYTHON%" portfolio_backtest.py --force
goto DONE

:RUN_MARKET
echo.
echo [%TIME%] Market backtest 60 — incremental (all delta symbols)...
echo.
"%PYTHON%" market_backtest.py --cap 60
goto DONE

:RUN_MARKET_FORCE
echo.
echo [%TIME%] Market backtest 60 — FORCE full recompute (all delta symbols)...
echo  (Re-runs entire history from scratch against full market universe)
echo.
"%PYTHON%" market_backtest.py --cap 60 --force
goto DONE

:RUN_MARKET50
echo.
echo [%TIME%] Market backtest 50 — incremental (top 50 by TP upside)...
echo.
"%PYTHON%" market_backtest.py --cap 50
goto DONE

:RUN_MARKET50_FORCE
echo.
echo [%TIME%] Market backtest 50 — FORCE full recompute...
echo.
"%PYTHON%" market_backtest.py --cap 50 --force
goto DONE

:RUN_MARKET40
echo.
echo [%TIME%] Market backtest 40 — incremental (top 40 by TP upside)...
echo.
"%PYTHON%" market_backtest.py --cap 40
goto DONE

:RUN_MARKET40_FORCE
echo.
echo [%TIME%] Market backtest 40 — FORCE full recompute...
echo.
"%PYTHON%" market_backtest.py --cap 40 --force
goto DONE

:RUN_MARKET30
echo.
echo [%TIME%] Market backtest 30 — incremental (top 30 by TP upside)...
echo.
"%PYTHON%" market_backtest.py --cap 30
goto DONE

:RUN_MARKET30_FORCE
echo.
echo [%TIME%] Market backtest 30 — FORCE full recompute...
echo.
"%PYTHON%" market_backtest.py --cap 30 --force
goto DONE

:RUN_SCORES
echo.
echo [%TIME%] Computing scores — incremental (RS + RVR + Band Position)...
echo.
"%PYTHON%" scoring.py
goto DONE

:RUN_SCORES_FORCE
echo.
echo [%TIME%] Computing scores — FORCE full recompute (all weeks)...
echo.
"%PYTHON%" scoring.py --force
goto DONE

:RUN_ETF
echo.
echo [%TIME%] Fetching sector ETF data — incremental (11 SPDR ETFs + SPY)...
echo.
"%PYTHON%" fetch_etf_data.py
goto DONE

:RUN_ETF_FORCE
echo.
echo [%TIME%] Fetching sector ETF data — FORCE full history re-fetch...
echo.
"%PYTHON%" fetch_etf_data.py --force
goto DONE

:RUN_CAPS
echo.
echo [%TIME%] Fetching market caps — incremental (skip symbols fetched in last 30 days)...
echo  This may take 20-40 mins for first run (6700 symbols @ 4 workers).
echo.
"%PYTHON%" fetch_market_caps.py --workers 4
goto DONE

:RUN_CAPS_FORCE
echo.
echo [%TIME%] Fetching market caps — FORCE re-fetch all symbols...
echo  This will take 20-40 mins (6700 symbols @ 4 workers).
echo.
"%PYTHON%" fetch_market_caps.py --force --workers 4
goto DONE

:RUN_SNP60
echo.
echo [%TIME%] SNP backtest 60 — incremental (S&P 500 members only)...
echo.
"%PYTHON%" snp_backtest.py --cap 60
goto DONE

:RUN_SNP60_FORCE
echo.
echo [%TIME%] SNP backtest 60 — FORCE full recompute (S&P 500 members only)...
echo.
"%PYTHON%" snp_backtest.py --cap 60 --force
goto DONE

:RUN_SNP50
echo.
echo [%TIME%] SNP backtest 50 — incremental...
echo.
"%PYTHON%" snp_backtest.py --cap 50
goto DONE

:RUN_SNP50_FORCE
echo.
echo [%TIME%] SNP backtest 50 — FORCE full recompute...
echo.
"%PYTHON%" snp_backtest.py --cap 50 --force
goto DONE

:RUN_SNP40
echo.
echo [%TIME%] SNP backtest 40 — incremental...
echo.
"%PYTHON%" snp_backtest.py --cap 40
goto DONE

:RUN_SNP40_FORCE
echo.
echo [%TIME%] SNP backtest 40 — FORCE full recompute...
echo.
"%PYTHON%" snp_backtest.py --cap 40 --force
goto DONE

:RUN_SNP30
echo.
echo [%TIME%] SNP backtest 30 — incremental...
echo.
"%PYTHON%" snp_backtest.py --cap 30
goto DONE

:RUN_SNP30_FORCE
echo.
echo [%TIME%] SNP backtest 30 — FORCE full recompute...
echo.
"%PYTHON%" snp_backtest.py --cap 30 --force
goto DONE

:RUN_SNP_EXPORT60
echo.
echo [%TIME%] Exporting SNP cap 60 trades to Excel...
echo.
"%PYTHON%" export_snp_trades.py --cap 60
goto DONE

:RUN_SNP_EXPORT50
echo.
echo [%TIME%] Exporting SNP cap 50 trades to Excel...
echo.
"%PYTHON%" export_snp_trades.py --cap 50
goto DONE

:RUN_SNP_EXPORT40
echo.
echo [%TIME%] Exporting SNP cap 40 trades to Excel...
echo.
"%PYTHON%" export_snp_trades.py --cap 40
goto DONE

:RUN_SNP_EXPORT30
echo.
echo [%TIME%] Exporting SNP cap 30 trades to Excel...
echo.
"%PYTHON%" export_snp_trades.py --cap 30
goto DONE

:RUN_VACUUM
echo.
echo [%TIME%] Running VACUUM on daily + weekly tables...
echo  (Reclaims disk space from stale parquet files)
echo.
"%PYTHON%" -c "import sys; sys.path.insert(0, '.'); import delta_utils; [delta_utils.vacuum_table(p, retention_hours=0) or print(f'  {p}: vacuumed') for p in ('daily', 'weekly')]"
echo.
echo Vacuum complete.
goto DONE

:RUN_SNAPSHOT
echo.
echo [%TIME%] Rebuilding weekly snapshot table...
echo.
"%PYTHON%" -c "import sys; sys.path.insert(0, '.'); import delta_utils; n = delta_utils.write_snapshot(); print(f'  Snapshot rebuilt: {n} symbols')"
goto DONE

:RUN_FIX_PRICES
echo.
echo [%TIME%] Scanning for and removing yfinance price corruption (smart mode)...
echo  (Detects corrupted symbols, deletes bad rows, resets state)
echo  (After this completes, run option 1 to re-fetch correct prices)
echo.
"%PYTHON%" fix_bad_prices.py
goto DONE

:RUN_FIX_ALL
echo.
echo [%TIME%] NUCLEAR: wiping ALL data from 2026-04-13 onwards for every symbol...
echo  (Deletes ALL rows from that date regardless of corruption status)
echo  (After this completes, run option 1 to re-fetch correct prices)
echo.
"%PYTHON%" fix_bad_prices.py --all
goto DONE

:DONE
echo.
if %ERRORLEVEL%==0 (
    echo ============================================================
    echo  Done successfully.  %DATE%  %TIME%
    echo ============================================================
) else (
    echo ============================================================
    echo  Finished with errors. Check logs\fetch.log for details.
    echo ============================================================
)
echo.

:END
pause
