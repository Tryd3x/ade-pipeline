.PHONY: up down

up: ## Spin up spark cluster and jupyter containers
	make down && docker compose up

down: ## Tear down spark cluster and jupyter containers
	docker compose down

down-hard: ## Tear down spark cluster, jupyter containers and volumes
	docker compose down -v

# run-scaled: ## Spin n spark-workers
# 	make down && docker compose up --scale worker=$(n)