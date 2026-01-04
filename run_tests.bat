@echo off
set TEST_MODE=1
echo Running tests in TEST_MODE...
python -m pytest -s tests/earnings_data/test_earnings_data_scraper.py > test_output.txt 2>&1
if errorlevel 1 (
    echo Test Failed
    exit /b 1
) else (
    echo Test Passed
    exit /b 0
)
