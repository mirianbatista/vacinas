FROM apache/airflow:2.2.5
COPY requirements.txt .
RUN pip install -r requirements.txt