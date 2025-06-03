DBT_CORE_VERSION="1.9.6"
DBT_BIGQUERY_VERSION="1.9.2"

docker build \
    --build-arg dbt_core_version="${DBT_CORE_VERSION}" \
    --build-arg dbt_bigquery_version="${DBT_BIGQUERY_VERSION}" \
    -f dbt-base.Dockerfile \
    -t dbt-base .