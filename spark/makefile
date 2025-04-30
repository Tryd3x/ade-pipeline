.PHONY: up down down-hard spark-submit spark-submit-year spark-install-requirements

up: ## Spin up spark cluster and jupyter containers
	make down && docker compose up

down: ## Tear down spark cluster and jupyter containers
	docker compose down

down-hard: ## Tear down spark cluster, jupyter containers and volumes
	docker compose down -v

spark-submit:
	docker exec -i spark-master bash -c 'spark-submit $$SHARED_WORKSPACE/$(JOB)'

spark-submit-year:
	docker exec -i spark-master bash -c 'spark-submit $$SHARED_WORKSPACE/$(JOB) --only_years=$(YEARS)'

spark-install-requirements:
	docker exec -i jupyterlab pip install -r requirements.txt
	
# run-scaled: ## Spin n spark-workers
# 	make down && docker compose up --scale worker=$(n)