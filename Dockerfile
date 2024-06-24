
FROM python:3.12-slim


WORKDIR /app

COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt


COPY . .

ENV DB_PASSWORD=zxcvbnm,./123

CMD ["python", "fetch_data.py"]
