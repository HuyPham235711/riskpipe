up:
	docker compose up --build -d

down:
	docker compose down

run-transaction-attribution-job:
	docker exec jobmanager ./bin/flink run --python ./code/transaction_attribution.py

sleep:
	sleep 20 

####################################################################################################################
# Testing, auto formatting, type checks, & Lint checks

format:
	docker exec datagen python -m black -S --line-length 130 .

isort:
	docker exec datagen isort .

type:
	docker exec datagen mypy --ignore-missing-imports --no-implicit-optional /opt

lint:
	docker exec datagen flake8 /opt --max-line-length=130

ci: isort format type lint

####################################################################################################################
# Run ETL

pyflink: 
	docker exec -ti jobmanager ./bin/pyflink-shell.sh local

run: down up sleep ci run-transaction-attribution-job

####################################################################################################################
# Monitoring

viz: 
	open http://localhost:3000

ui:
	open http://localhost:8081/