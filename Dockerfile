FROM python:3.12-slim

# Install zbar library for QR code decoding
RUN apt-get update && apt-get install -y libzbar0 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 10000

# Run with gunicorn
CMD ["gunicorn", "server_datatourisme_postgres:app", "--bind", "0.0.0.0:10000", "--timeout", "120", "--workers", "2"]
