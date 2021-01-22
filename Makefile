include .env

setup:
	docker-compose up -d --force-recreate --remove-orphans
	sleep 240
	docker exec airflow-webserver airflow users create --username admin --password admin --role Admin --firstname Ademir --lastname Junior --email admin@email.com
	docker exec airflow-webserver airflow connections add 'source' --conn-uri 'postgresql://root:root@source-db:5432/source'
	docker exec airflow-webserver airflow connections add 'dest' --conn-uri 'postgresql://root:root@dest-db:5432/dest'
	docker exec handler python insert_initial_data.py

down:
	docker-compose down

run: 
	docker exec airflow-webserver airflow dags backfill -s 2019-01-01 sync_source_dest_incremental
	
tests:
	docker exec handler pytest -vv
