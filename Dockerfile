FROM apache/airflow:2.5.3
USER airflow

RUN pip install --no-cache-dir pandas apache-airflow-providers-mongo