FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code and the SQLite DB 
COPY app.py ./ 
COPY data/Database1.db ./  
# App config
ENV PORT=5002
ENV DB_PATH=/app/Database1.db   
EXPOSE 5002

# Start the app
CMD ["gunicorn", "-w", "2", "-k", "gthread", "-b", "0.0.0.0:5002", "app:app"]
