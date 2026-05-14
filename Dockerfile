FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=3000 \
    DB_PATH=/data/queue.db

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY queue_bot ./queue_bot

RUN mkdir -p /data

EXPOSE 3000

CMD ["python", "-m", "queue_bot"]
