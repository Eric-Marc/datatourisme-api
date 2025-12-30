FROM python:3.11-slim

# Installer libzbar pour pyzbar
RUN apt-get update && apt-get install -y \
    libzbar0 \
    libzbar-dev \
    && rm -rf /var/lib/apt/lists/*

# Définir le chemin de la librairie
ENV LD_LIBRARY_PATH=/usr/lib:/usr/local/lib

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 10000

CMD ["gunicorn", "--bind", "0.0.0.0:10000", "--timeout", "120", "server_datatourisme_postgres:app"]
