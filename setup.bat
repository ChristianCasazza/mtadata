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
    echo Note: This is the default community key. Please use your own key if possible.
)

REM Step 5: Copy .env.example to .env
copy .env.example .env >nul
IF ERRORLEVEL 1 (
    echo Error: Failed to copy .env.example to .env
    exit /b 1
)

REM Step 6: Run exportpathwindows.py to generate LAKE_PATH
FOR /F "delims=" %%i IN ('uv run scripts/exportpathwindows.py') DO SET LAKE_PATH=%%i
IF "%LAKE_PATH%"=="" (
    echo Error: Failed to generate LAKE_PATH
    exit /b 1
)

REM Step 7: Add SOCRATA_API_TOKEN and LAKE_PATH to .env
echo SOCRATA_API_TOKEN=%SOCRATA_API_TOKEN% >> .env
echo LAKE_PATH=%LAKE_PATH% >> .env

REM Step 8: Run Dagster development server
echo Starting Dagster development server...
dagster dev
