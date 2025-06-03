FROM python:3.10-slim

ARG dbt_core_version
ARG dbt_bigquery_version

ENV DBT_CORE_VERSION=${dbt_core_version}
ENV DBT_BIGQUERY_VERSION=${dbt_bigquery_version}

RUN apt-get update && \
    apt-get install -y \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-core==${DBT_CORE_VERSION} dbt-bigquery==${DBT_BIGQUERY_VERSION}

WORKDIR /opt/dbt

ENTRYPOINT [ "dbt" ]