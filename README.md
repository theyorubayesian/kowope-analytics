# Kowope Analytics

A minimal implementation of [DBT](https://docs.getdbt.com/docs/introduction) to transform data for a Superstore. We start by normalizing the provided dataset into fact and dimension tables. Then we define DBT models to combine and/or transform the data as needed.

## Setup
This repository has only been tested with `Python 3.8`. After cloning the repository, create a project environment using `make` as follows
```shell
make create-env
```
Otherwise, you can run the individual commands used in the [Makefile](Makefile):
```shell
python -m venv env
pip install --upgrade pip
pip install -r requirements.txt
cp .envexample .env
```

## Local Development

* You may need to install additional development requirements. See [dev-requirements.txt](dev-requirements.txt).
* Start a Postgres Server locally and create a Database. See [Postgres: Starting the Database Server](https://www.postgresql.org/docs/current/server-start.html) &  [How to start PostgreSQL 12.4 [OSX]](https://dba.stackexchange.com/questions/274334/how-to-start-postgresql-12-4-osx)
* It is advisable to create/configure a database user (`dbt_user`) for this project & grant it all necessary permissions to your database.
* Fill in your database credentials in [.env](.env) file created. 
* Create a new schema to hold the tables we will work with.
* Run `make_star_schema.py` to create three tables: `dim_customer`, `dim_product` and `fact_order`.
```shell
python easysolar/utils/make_star_schema.py --csv-file /path/to/csv/file
```
* Validate that the three tables are created in Postgres.
* Configure your DBT Profile to include this DBT Project. See [DBT: Connection profiles](https://docs.getdbt.com/docs/get-started/connection-profiles) & [Configuring DBT Profiles](https://timeflow.academy/dbt/labs/configuring-dbt-profiles)
* Validate your profile using `dbt debug` in terminal
* Install dbt packages
```
make install-deps
```
* Run dbt transformations & serve the documentation server
```
make run
make docs
```

Note that you can run all dbt commands at once using
```
make dbt-all
```

## TODO
- Attempt more complex pivot transformations
- Build incremental dbt models
- Scheduling with Prefect or Airflow
