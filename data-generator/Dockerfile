FROM python:3.11-slim

# Install system deps for confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generator.py .
COPY healthcheck.py .

CMD ["python", "-u", "generator.py"]
