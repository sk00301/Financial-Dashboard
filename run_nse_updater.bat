@echo off
REM NSE Data Updater - Batch Script for Windows Task Scheduler
REM This script activates the virtual environment and runs the updater

REM Change to the project directory
cd /d "%~dp0"

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Run the updater in 'once' mode
python nse_data_updater.py once

REM Deactivate virtual environment
call venv\Scripts\deactivate.bat

REM Exit
exit