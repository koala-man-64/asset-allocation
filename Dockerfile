FROM mcr.microsoft.com/playwright/python:v1.57.0-jammy

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY scripts/ scripts/

# Set Python path to ensure scripts can be run as modules
ENV PYTHONPATH=/app

# Default entrypoint (will be overridden by ACA Job command)
CMD ["python", "-m", "scripts.market_data.bronze_market_data"]
