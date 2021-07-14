# Airflow DAGs for LITE

DAGs go into the [dags](./dags) subdirectory.

## Running airflow locally (test and development)

### Pre-requisite

You will need a postgres server running locally which you can use for lite-api and airflow, when testing. You will also
need credentials to connect to an S3 bucket (stored in vcap.json).

1. Copy .env.example to .env and put in real values
   `cp .env.example .env`
2. Copy example_vcap.json to vcap.json and put in real values
   `cp example_vcap.json vcap.json`

### Cleaning up from previous test-runs

Disconnect, drop and re-create any previous airflow databases before setting up airflow, for example:

```bash
psql -h localhost -p 5462 -U postgres
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'airflow';
DROP database airflow;
CREATE database airflow;
```

Kill any other processes hogging port 8080:

```bash
netstat -vanp tcp | grep pid
netstat -vanp tcp | grep 8080
kill -9 the_pid
```

### Running Airflow

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
export $(grep -v '^#' .env | xargs)  # Export the .env file
export VCAP_SERVICES=$(cat vcap.json)  # Export the vcap.json
airflow db init
airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
airflow scheduler > scheduler_log.log &
airflow webserver --port 8080
```

### Uploading the anonymised data

You can manually test the dump_anonymiser by using a local copy of the 
lite-api database.

1. Run the database. `docker-compose up db`
1. Connect and recreate the database. `psql -h localhost -p 5462 -U postgres`, 
   followed by `drop database lite-api;` and then `create database lite-api;`.
1. Upload the anonymised data.  
   `PGUSER='postgres' PGPASSWORD='password' psql -d lite-api -h localhost -p 5462 -f anonymised.sql`

## Running in Docker (version 1 of airflow only)

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



