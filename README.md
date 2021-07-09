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

#### .env Values

If you don't know what values to use in the .env file there is a copy of the
real values for local development in Vault

### Connections

You will need to configure a couple of connections in the Airflow UI, this can
be done at http://localhost:8080/

#### spire_local

_target database for spire to postgres conversion_

If you are using docker-compose you won't need to configure this as the .env file
has the correct value to automatically configure this.

Manual setup:
Go to [http://localhost:8080/admin/connection/](http://localhost:8080/admin/connection/) and create a
new connection with the:

| Field     | Value         | Description                             |
| --------- | ------------- | --------------------------------------- |
| Conn Id   | `spire_local` | postgres host                           |
| Conn Type | `Postgres`    |                                         |
| Schema    | `postgres`    | Or whatever the target database name is |
| Login     | `postgres`    | Or whatever the username is             |
| Password  | `password`    | Or whatever the password is             |
| Port      | `5432`        | 5432 is the default                     |
| Extra     |               | Leave blank                             |

#### aws_default

_connection info for connecting to s3, where spire csvs are stored_

This one is not configured with environment variables even if using the `.env` file and should be
manually set up.

Go to [http://localhost:8080/admin/connection/](http://localhost:8080/admin/connection/) and edit a
connection that already exists called `aws_default`:

| Field     | Value                                                   | Description                                                                        |
| --------- | ------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| Conn Id   | `aws_default`                                           | leave as is                                                                        |
| Conn Type | `Amazon Web Services`                                   | leave as is                                                                        |
| Schema    |                                                         | leave blank                                                                        |
| Login     | `A…`                                                    | AWS Access Key ID                                                                  |
| Password  | `…`                                                     | AWS Amazon Secret Access Key                                                       |
| Port      |                                                         | leave blank                                                                        |
| Extra     | `{"region_name": "eu-west-2", "bucket_name": "bucket"}` | bucket_name is the name of the s3 bucket you want the spire csvs to be dumped into |

These values can be obtained using the cloudfoundry `cf` cli:
`cf service-key SERVICE_KEY_NAME SERVICE_NAME`.

Ask another developer for the service key name / service name
or enumerate services in the space - the name should be obvious
(and has the word `csv` in it, type should be `s3`).

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

## Running airflow locally

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
export $(grep -v '^#' .env | xargs)  # Export the .env file
airflow db init
airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
airflow scheduler > scheduler_log.log &
airflow webserver --port 8080
```
TH
## Testing export_lite_db

You can manually test the dump_anonamiser by using a local copy of the 
lite-api database.

1. Delete the db from docker. In the lite-api directory run `docker-compose rm db`
1. Run up a new copy of the database. `docker-compose up db`
1. Connect and confirm that is is empty. `psql -h localhost -p 5462 -d lite-api -U postgres`, 
   followed by `\dt`.
1. Upload the anonamised data.  
   `PGUSER='postgres' PGPASSWORD='password' psql -d lite-api -h localhost -p 5462 -f anonymised.sql`
```