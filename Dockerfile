FROM python:3.8.13

# Устанавливаем зависимости без --user
RUN pip install \
    apache-airflow==2.9.2 \
    psycopg2-binary==2.9.9 \
    clickhouse-connect==0.6.5 \
    redis==5.0.7

RUN mkdir -p /usr/local/airflow/dags
WORKDIR /usr/local/airflow
ENV AIRFLOW_HOME=/usr/local/airflow
ENV PATH=/root/.local/bin:$PATH

COPY airflow.cfg /usr/local/airflow/airflow.cfg
COPY lab5_dag.py /usr/local/airflow/dags/lab.py

