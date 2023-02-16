FROM apache/airflow:2.5.1-python3.10

USER root

RUN apt-get update
RUN apt install -y build-essential

USER airflow

