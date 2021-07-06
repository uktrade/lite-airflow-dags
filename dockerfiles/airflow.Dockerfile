FROM puckel/docker-airflow:1.10.9

USER root
RUN apt-get update && apt-get install -y \
  alien \
  libaio1 \
  && rm -rf /var/lib/apt/lists/*

RUN curl -L \
    https://download.oracle.com/otn_software/linux/instantclient/199000/oracle-instantclient19.9-basic-19.9.0.0.0-1.x86_64.rpm \
    -o /tmp/oracle-instantclient19.9-basic-19.9.0.0.0-1.x86_64.rpm \
    && alien -i /tmp/oracle-instantclient19.9-basic-19.9.0.0.0-1.x86_64.rpm \
    && rm /tmp/oracle-instantclient19.9-basic-19.9.0.0.0-1.x86_64.rpm

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

USER airflow
