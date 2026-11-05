# DataTrust runtime image: Python 3.11 + Java 17 (for Spark) + project dependencies.
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    DATATRUST_DATA_ROOT=/app/data

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt requirements-dashboard.txt ./
RUN pip install --no-cache-dir -r requirements-dashboard.txt

COPY . .
RUN useradd --create-home datatrust && mkdir -p /app/data && chown -R datatrust /app
USER datatrust
CMD ["python", "-m", "src.cli", "status"]
