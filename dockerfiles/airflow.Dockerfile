FROM puckel/docker-airflow:latest

USER root
RUN apt-get update && apt-get install -y \
  alien \
  libaio1 \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

USER airflow
