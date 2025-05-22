# Use Python 3.9 slim base
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port (if you're running a web server like Flask + Gunicorn)
EXPOSE 8080

# Default command (adjust if you're running CLI instead of web app)
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--timeout", "300", "flask_app:app"]
