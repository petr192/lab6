FROM python:3.8.13

RUN pip install --user psycopg2-binary==2.9.9 apache-airflow==2.9.2 clickhouse-driver==0.2.8 redis==5.0.7

RUN mkdir -p /usr/local/airflow/dags
WORKDIR /usr/local/airflow
ENV AIRFLOW_HOME=/usr/local/airflow
ENV PATH=/root/.local/bin:$PATH

COPY airflow.cfg /usr/local/airflow/airflow.cfg
COPY home/ubuntu/lab6/lab5_dag.py /usr/local/airflow/dags/lab.py
