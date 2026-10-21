FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py ./main.py
COPY storage ./storage
COPY ingestion ./ingestion
COPY pipeline ./pipeline
COPY run_stage1.py ./run_stage1.py
COPY batching.py ./batching.py
COPY prepare_target_universe.py ./prepare_target_universe.py
COPY run_stage1_batch.py ./run_stage1_batch.py
COPY run_stage2.py ./run_stage2.py
COPY run_stage3.py ./run_stage3.py
COPY tests ./tests

CMD ["python", "main.py"]

