# Use Python 3.9 as base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install required system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create directories
RUN mkdir -p lineups

# Create the directory for the database if it doesn't exist
RUN mkdir -p /app/database

# Set permissions for the database directory (adjust user if necessary)
# This example gives read/write to all for simplicity in this context,
# but you should be more restrictive in a production environment.
RUN chmod -R 777 /app/database

# Set environment variables for Chrome/Selenium
ENV PYTHONUNBUFFERED=1
ENV DISPLAY=:99
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV SELENIUM_HEADLESS=true

# Create a non-root user to run the application
RUN useradd -m appuser
RUN chown -R appuser:appuser /app
USER appuser

# Expose the port the app runs on
EXPOSE 5000

# Command to run the application
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--timeout", "300", "flask_app:app"]