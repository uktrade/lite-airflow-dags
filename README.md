# Airflow DAGs for LITE

## Local Development

Write your DAGs in the [pipelines](./pipelines) subdirectory.

### Pre-requsites

To run this repository in development mode it requires Docker and docker-compose.

The local development version **doesn't use**:

- CeleryExecutor so it doesn't need redis
- Multiple workers so it doesn't need a Postgres database

### Running

To run locally it uses the [puckel/docker-airflow](https://github.com/puckel/docker-airflow)
distribution.

`docker-compose up -d` will start Airflow at http://localhost:8080

### Running commands

You can run commands in the airflow container e.g.

`docker-compose run webserver airflow list_dags`

`docker-compose run webserver ipython`

### Extra Python Packages

Add them to [requirements.txt](./requirements.txt), the container will
install them on startup.

If you want to run commands in the running container instead of a new one then
use exec instead e.g.:

`docker-compose exec webserver airflow list_dags`

## Production

In production this repository is pulled into a subdirectory of the working tree
of the paas-airflow project. The paas-airflow project sees the pipelines and
loads them into the airflow dag bag.

You will need to configure any connections you have set up locally in the paas-airflow
admin interface.
