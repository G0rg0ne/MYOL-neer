FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for Playwright/Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    libatspi2.0-0 \
    libxshmfence1 \
    fonts-liberation \
    fonts-noto-color-emoji \
    fonts-unifont \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast, reliable dependency management
RUN pip install --no-cache-dir uv

# Copy dependency files first (for better layer caching)
COPY pyproject.toml uv.lock ./
COPY config.yaml ./

# Install dependencies using uv (with local extras for playwright)
RUN uv sync --frozen --extra local

# Install Playwright browser (without --with-deps since we installed them above)
RUN uv run playwright install chromium

# Copy application code and package
COPY fast_flights/ ./fast_flights/
COPY flight_price_fetcher.py .
COPY test_flight.py .

# Create data directory
RUN mkdir -p /app/data

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Use uv run to execute commands within the virtual environment
ENTRYPOINT ["uv", "run"]

CMD ["python", "flight_price_fetcher.py"]
