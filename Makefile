.RECIPEPREFIX := >
.PHONY: init up up-minio down seed test lint fmt ci dbt-parse docs logs clean
COMPOSE := docker compose

init:
> @test -f .env || cp .env.example .env
> @mkdir -p data/raw logs dags warehouse gx gx_reports src dbt tests scripts docs
> @grep -q '^AIRFLOW_FERNET_KEY=.\+' .env || { sed -i.bak "s#^AIRFLOW_FERNET_KEY=.*#AIRFLOW_FERNET_KEY=$$(openssl rand -base64 32 | tr '+/' '-_')#" .env && rm -f .env.bak; }
> @grep -q '^AIRFLOW_SECRET_KEY=.\+' .env || { sed -i.bak "s#^AIRFLOW_SECRET_KEY=.*#AIRFLOW_SECRET_KEY=$$(openssl rand -hex 32)#" .env && rm -f .env.bak; }

up: init
> $(COMPOSE) up -d --build

up-minio: init
> $(COMPOSE) --profile minio up -d --build

down:
> $(COMPOSE) --profile minio down

seed:
> $(COMPOSE) exec airflow-scheduler airflow dags trigger afrishop_daily_pipeline

test:
> $(COMPOSE) exec airflow-worker python -m pytest /opt/airflow/tests -q

lint:
> $(COMPOSE) exec airflow-worker bash -lc 'cd /opt/airflow && black --check --line-length=100 dags src tests && isort --check-only --profile=black --line-length=100 dags src tests && ruff check dags src tests && mypy --config-file=pyproject.toml'

fmt:
> $(COMPOSE) exec airflow-worker bash -lc 'cd /opt/airflow && black --line-length=100 dags src tests && isort --profile=black --line-length=100 dags src tests && ruff check --fix dags src tests'

dbt-parse:
> $(COMPOSE) exec airflow-worker bash -lc 'cd /opt/airflow/dbt && /home/airflow/dbt_venv/bin/dbt parse'

ci: lint test dbt-parse

docs:
> $(COMPOSE) exec airflow-worker bash -c "cd /opt/airflow/dbt && /home/airflow/dbt_venv/bin/dbt docs generate"

logs:
> $(COMPOSE) logs -f --tail=100

clean:
> $(COMPOSE) --profile minio down -v --remove-orphans
