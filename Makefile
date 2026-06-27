.PHONY: up down produce consume rollup dbt test clean

up:
	docker compose up -d
	@echo "✅ Kafka running on localhost:9092"

down:
	docker compose down

produce:
	python -m src.producer.event_producer --rate 50

consume:
	python -m src.streaming.stream_consumer

rollup:
	python -m src.batch.hourly_rollup

dbt:
	cd dbt && dbt build --profiles-dir .

test:
	pytest tests/ -v

clean:
	rm -rf lakehouse/ checkpoints/ spark-warehouse/ metastore_db/ derby.log dbt/target dbt/logs
	@echo "🧹 cleaned"
