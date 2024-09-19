FROM python:3.10-slim

WORKDIR /app/data_pipeline_services

RUN apt-get update && apt-get install -y libpq-dev gcc

COPY data_processing/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

CMD ["python", "data_processing/main.py"]