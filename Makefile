.PHONY: create-env create-dev-env clean install-deps run docs

create-env:
	python -m venv env
	pip install --upgrade pip
	pip install -r requirements.txt
	cp .envexample .env

create-dev-env:	create-env
	pip install -r dev-requirements.text

clean:
	cd easysolar && dbt clean

install-deps:
	cd easysolar && dbt deps

run:
	cd easysolar && dbt run

docs:
	cd easysolar && dbt docs generate && dbt docs serve

dbt-freshen: clean install-deps

dbt-all: clean install-deps run docs
