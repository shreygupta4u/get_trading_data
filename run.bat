@echo off
title NASDAQ Candle Fetcher

set PYTHON=C:\Users\mohin\AppData\Local\Python\pythoncore-3.14-64\python.exe
set DIR=C:\Users\mohin\Documents\PythonCodes\get_trading_data

cd /d "%DIR%\src"
if not exist "..\logs" mkdir "..\logs"

echo.
echo ============================================================
echo  NASDAQ Candle Fetcher
echo  %DATE%  %TIME%
echo ============================================================
echo.
echo  Options:
echo    1. Run normally       (incremental - only fetch new data)
echo    2. Run with --force   (re-fetch everything from scratch)
echo    3. Daily only
echo    4. Weekly only
echo    5. Exit
echo.
set /p CHOICE="Enter choice [1-5]: "

if "%CHOICE%"=="1" goto RUN_NORMAL
if "%CHOICE%"=="2" goto RUN_FORCE
if "%CHOICE%"=="3" goto RUN_DAILY
if "%CHOICE%"=="4" goto RUN_WEEKLY
if "%CHOICE%"=="5" goto END

echo Invalid choice. Running normally...

:RUN_NORMAL
echo.
echo [%TIME%] Starting incremental run (daily + weekly)...
echo.
"%PYTHON%" orchestrator.py
goto DONE

:RUN_FORCE
echo.
echo [%TIME%] Starting FULL re-fetch (this may take a long time)...
echo.
"%PYTHON%" orchestrator.py --force
goto DONE

:RUN_DAILY
echo.
echo [%TIME%] Running daily period only...
echo.
"%PYTHON%" orchestrator.py --period daily
goto DONE

:RUN_WEEKLY
echo.
echo [%TIME%] Running weekly period only...
echo.
"%PYTHON%" orchestrator.py --period weekly
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
