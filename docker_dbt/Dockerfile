FROM python:3.11-slim

WORKDIR /usr/app/dbt

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "dbt-core==1.12.6" "dbt-bigquery==1.12.1"

ENTRYPOINT ["dbt"]