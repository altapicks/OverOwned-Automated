FROM python:3.11-slim

# System packages for asyncpg / other native deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (leverages Docker layer cache)
COPY pyproject.toml .
RUN pip install --upgrade pip && pip install -e .

COPY . .

# Railway sets $PORT. Default for local use is 8000.
ENV PORT=8000
EXPOSE 8000

# Default CMD: run the API. Railway worker service overrides this to run the watcher.
CMD uvicorn app.main:app --host 0.0.0.0 --port $PORT
