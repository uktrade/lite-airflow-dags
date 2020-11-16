# Airflow DAGs for LITE

## Local Development

Write your DAGs in the [pipelines](./pipelines) subdirectory.

### Pre-requsites

To run this repository in development mode it requires Docker and docker-compose.

The local development version **doesn't use**:

- LocalExecutor so it doesn't need redis

### Running

To run locally it uses the [puckel/docker-airflow](https://github.com/puckel/docker-airflow)
distribution.

1. Copy .env.example to .env and put in real values
   `cp .env.example .env`

2. Connect to DIT VPN

3. `docker-compose up -d` will start Airflow at http://localhost:8080 along with a
   supporting postgres database and a ssh-tunnel to the oracle replica.

### Running commands

You can run commands in the airflow container e.g.

`docker-compose run webserver airflow list_dags`

`docker-compose run webserver python`

If you want to run commands in the running container instead of a new one then
use exec instead e.g.:

`docker-compose exec webserver airflow list_dags`

### Extra Python Packages

1. Add them to [requirements.in](./requirements.in)
2. run [pip-compile](https://github.com/jazzband/pip-tools) to generate a `requirements.txt`

the container will
install them on startup.

## Production

In production this repository is pulled into a subdirectory of the working tree
of the paas-airflow project. The paas-airflow project sees the pipelines and
loads them into the airflow dag bag.

You will need to configure any connections you have set up locally in the paas-airflow
admin interface.
