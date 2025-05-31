This module contains the following:
- Images for the spark cluster (spark master and spark workers)
- Images for jupyterlab and livy (for job submissions)
- Spark jobs and notebooks
- Bind mount `volumes` for the spark cluster
- Makefile to spin the spark cluster up and submit spark jobs with optional parameters
- Ensure that an external docker network `shared_network` exists before spinning them up

Spin up the spark cluster:
```
docker compose up
```