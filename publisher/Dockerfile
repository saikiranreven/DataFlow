FROM python:3.10-slim
RUN pip install --no-cache-dir flask google-cloud-pubsub
COPY main.py /app/main.py
WORKDIR /app
CMD ["python", "main.py"]