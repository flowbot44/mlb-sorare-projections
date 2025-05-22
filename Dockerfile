FROM python:3.9-slim-bullseye

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    gnupg \
    curl \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxss1 \
    libxtst6 \
    lsb-release \
    xdg-utils \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Download "Chrome for Testing" bundle (v136.x stable)
ENV CHROME_VERSION=136.0.7103.113
RUN curl -sSL "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/${CHROME_VERSION}/linux64/chrome-linux64.zip" -o chrome.zip && \
    curl -sSL "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/${CHROME_VERSION}/linux64/chromedriver-linux64.zip" -o chromedriver.zip && \
    unzip chrome.zip && \
    unzip chromedriver.zip && \
    mv chrome-linux64 /opt/chrome && \
    mv chromedriver-linux64/chromedriver /usr/bin/chromedriver && \
    chmod +x /usr/bin/chromedriver && \
    rm -rf *.zip chromedriver-linux64

# Set environment variables
ENV CHROME_BIN=/opt/chrome/chrome
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Install Python dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Expose and run
EXPOSE 8080
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--timeout", "300", "flask_app:app"]
