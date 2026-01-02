FROM python:3.11-slim

# OpenCV headless n'a pas besoin de libzbar
# Mais peut avoir besoin de libgl pour certaines ops
RUN apt-get update && apt-get install -y libgl1-mesa-glx libglib2.0-0 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 10000

CMD ["gunicorn", "--bind", "0.0.0.0:10000", "--timeout", "120", "server_datatourisme_postgres:app"]
