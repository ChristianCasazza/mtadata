@echo off

REM Step 1: Create the virtual environment
uv venv
IF NOT EXIST ".venv\Scripts\activate" (
    echo Error: Virtual environment not found at .venv\Scripts\activate
    exit /b 1
)

REM Step 2: Activate the virtual environment
call .venv\Scripts\activate || (
    echo Error: Failed to activate virtual environment
    exit /b 1
)

REM Step 3: Install dependencies
uv sync
IF ERRORLEVEL 1 (
    echo Error: Failed to sync dependencies
    exit /b 1
)

REM Step 4: Ask user for SOCRATA_API_TOKEN
set /p SOCRATA_API_TOKEN=Please enter your SOCRATA_API_TOKEN (press Enter to use the default community key):
IF "%SOCRATA_API_TOKEN%"=="" (
    set SOCRATA_API_TOKEN=uHoP8dT0q1BTcacXLCcxrDp8z
    echo Note: Using the default community key. Please use your own key if possible.
)

REM Step 5: Create a fresh .env
del .env 2>nul
echo. > .env

REM Step 6: Run exportpathwindows.py to retrieve LAKE_PATH (line1) and DAGSTER_HOME (line2)
setlocal enabledelayedexpansion
set i=0
for /f "delims=" %%a in ('uv run scripts/exportpathwindows.py') do (
    if !i! == 0 (
        set LAKE_PATH=%%a
    ) else (
        set DAGSTER_HOME=%%a
    )
    set /a i+=1
)

if "%LAKE_PATH%"=="" (
    echo Error: Failed to retrieve LAKE_PATH
    exit /b 1
)
if "%DAGSTER_HOME%"=="" (
    echo Error: Failed to retrieve DAGSTER_HOME
    exit /b 1
)

REM Step 7: Append to .env
echo SOCRATA_API_TOKEN=%SOCRATA_API_TOKEN% >> .env
echo LAKE_PATH=%LAKE_PATH% >> .env
echo DAGSTER_HOME=%DAGSTER_HOME% >> .env

REM Step 8: Generate dagster.yaml in DAGSTER_HOME
if not exist "%DAGSTER_HOME%" mkdir "%DAGSTER_HOME%"
uv run scripts\generate_dagsteryaml.py "%DAGSTER_HOME%" > "%DAGSTER_HOME%\dagster.yaml"

REM Step 9: Start Dagster dev server
echo Starting Dagster development server...
dagster dev
