.PHONY: up down down-hard spark-submit spark-submit-year spark-install-requirements

up: ## Spin up spark cluster and jupyter containers
	make down && docker compose up

up-d: ## Spin up spark cluster and jupyter containers
	make down && docker compose up -d

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
	    process_raw_layer/main.py --year=$(YEARS)'

spark-submit-year:
	docker exec -i spark-master bash -c 'spark-submit /opt/workspace/jobs/$(JOB) --year=$(YEARS)'

# spark-local-submit:
# 	cd jobs && \
# 		zip -r process_raw_layer.zip process_raw_layer -x "*__pycache__*" && \
# 		spark-submit \
# 			--py-files process_raw_layer.zip \
# 			--jars /home/hyderreza/codehub/ade-pipeline/spark/jobs/process_raw_layer/gcs-connector-hadoop3-2.2.14.jar \
# 			--conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
# 			--conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
# 			--conf spark.ui.port=4040 \
# 			process_raw_layer/main.py --year=$(YEARS)