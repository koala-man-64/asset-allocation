# Tasks-only image for Azure Container Apps Jobs.
# NOTE: API/UI are intentionally excluded from this image to minimize size and attack surface.
FROM python:3.10-slim-bookworm

WORKDIR /app

COPY requirements.lock.txt .
# Install Python dependencies first for better layer caching.
RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.lock.txt

# Copy application code required by jobs.
COPY pyproject.toml README.md ./
COPY alpha_vantage/ alpha_vantage/
COPY core/ core/
COPY monitoring/ monitoring/
COPY tasks/ tasks/
RUN pip install --no-cache-dir .

# Jobs override the command in their YAML; keep a benign default.
CMD ["python", "-c", "print('asset-allocation task image: specify a job command (e.g., python -m tasks.market_data.bronze_market_data)')"]
