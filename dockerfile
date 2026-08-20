FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY app.py ./
COPY config.py ./
COPY database.py ./
COPY cache.py ./
COPY utils.py ./
COPY routes/ ./routes/
COPY mock_overrides.json ./

# Copy the SQLite DBs
COPY data/Database1.db ./data/
COPY data/property_details.db ./data/

# App config
ENV PORT=5002
ENV DB_PATH=/app/data/Database1.db
ENV DETAILS_DB_PATH=/app/data/property_details.db
EXPOSE 5002

# Start the app
CMD ["gunicorn", "-w", "2", "-k", "gthread", "-b", "0.0.0.0:5002", "--timeout", "120", "app:app"]
