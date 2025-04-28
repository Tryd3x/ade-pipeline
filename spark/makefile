.PHONY: up down down-hard spark-submit

up: ## Spin up spark cluster and jupyter containers
	make down && docker compose up

down: ## Tear down spark cluster and jupyter containers
	docker compose down

down-hard: ## Tear down spark cluster, jupyter containers and volumes
	docker compose down -v

spark-submit:
	docker exec -i spark-master bash -c 'spark-submit $$SHARED_WORKSPACE/$(JOB)'
	
# run-scaled: ## Spin n spark-workers
# 	make down && docker compose up --scale worker=$(n)