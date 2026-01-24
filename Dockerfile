FROM mcr.microsoft.com/playwright/python:v1.57.0-jammy

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.lock.txt .
RUN pip install --no-cache-dir -r requirements.lock.txt

# Install application code as a package (avoids PYTHONPATH hacks).
COPY pyproject.toml README.md ./
COPY core/ core/
COPY tasks/ tasks/
COPY services/ services/
COPY alpaca/ alpaca/
COPY api/ api/
COPY backtest/ backtest/
COPY monitoring/ monitoring/
COPY scripts/ scripts/
RUN pip install --no-cache-dir .

# Default entrypoint (will be overridden by ACA Job command)
CMD ["python", "-m", "scripts.market_data.bronze_market_data"]
