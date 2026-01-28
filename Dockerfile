

# Base image includes Python + Playwright browsers used by data jobs.
FROM mcr.microsoft.com/playwright/python:v1.57.0-jammy

# Set working directory for application code.
WORKDIR /app

# Install runtime dependencies from the lockfile for reproducible builds.
COPY requirements.lock.txt .
RUN pip install --no-cache-dir -r requirements.lock.txt

# Install application code as a package (avoids PYTHONPATH hacks).
# This image is used for batch/data jobs (see deploy/job_*.yaml), not the API server.
COPY pyproject.toml README.md ./
COPY core/ core/
COPY tasks/ tasks/
COPY services/ services/
COPY alpaca/ alpaca/
COPY api/ api/
COPY monitoring/ monitoring/
# scripts/ is optional and not required for the API image; keep it in this job image if present.
COPY scripts/ scripts/
RUN pip install --no-cache-dir .

# Default entrypoint (will be overridden by ACA Job command).
CMD ["python", "-m", "tasks.market_data.bronze_market_data"]
