FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY opensky_producer.py ./
# COPY airports.json ./  # Removed hardcoded airports dataset

ENV PYTHONUNBUFFERED=1

CMD ["python", "opensky_producer.py"]
