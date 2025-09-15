# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
# If your DB file is local, ensure it's copied; or create it at runtime.

EXPOSE 5001
# shell form so ${PORT:-5001} expands
CMD gunicorn -w 2 -k gthread -b 0.0.0.0:${PORT:-5001} app:app
