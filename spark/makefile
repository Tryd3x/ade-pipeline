.PHONY: up down down-hard spark-submit spark-submit-year spark-install-requirements

up: ## Spin up spark cluster and jupyter containers
	make down && docker compose up

down: ## Tear down spark cluster and jupyter containers
	docker compose down

down-hard: ## Tear down spark cluster, jupyter containers and volumes
	docker compose down -v

spark-submit:
	docker exec -i spark-master bash -c '\
	  cd jobs && \
	  zip -r process_raw_layer.zip process_raw_layer -x "*__pycache__*" && \
	  spark-submit \
	    --py-files process_raw_layer.zip \
	    process_raw_layer/main.py --only_years=$(YEARS)'

# spark-submit:
# 	docker exec -i spark-master bash -c 'spark-submit /opt/workspace/$(JOB)'

spark-submit-year:
	docker exec -i spark-master bash -c 'spark-submit /opt/workspace/$(JOB) --only_years=$(YEARS)'