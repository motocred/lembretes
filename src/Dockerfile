FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENV PORT 8080

COPY . .

CMD ["python", "main.py"]
