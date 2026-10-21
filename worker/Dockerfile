FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py ./main.py
COPY storage ./storage
COPY tests ./tests

CMD ["python", "main.py"]

