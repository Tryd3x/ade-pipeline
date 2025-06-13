This module contains the following:
- Ingestion scripts and notebooks
- Makefile to spin the spark cluster up and submit spark jobs with optional parameters
- Ensure that an external docker network `shared_network` exists before spinning them up

Spin up the spark cluster:
```
docker compose up
```