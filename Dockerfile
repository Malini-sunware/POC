
FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .

ENV DB_PASSWORD=zxcvbnm,./123

CMD ["python", "fetch_data.py"]
