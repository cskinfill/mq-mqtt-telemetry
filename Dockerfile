FROM python:3

WORKDIR /opt/app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY mq-telemetry.py .

ENTRYPOINT ["python","mq-telemetry.py"]
