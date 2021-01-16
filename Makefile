up:
	docker-compose up -d --force-recreate --remove-orphans
	sleep 15
	docker exec airflow-webserver airflow users create --username admin --password admin --role Admin --firstname Ademir --lastname Junior --email admin@email.com
	docker exec airflow-webserver airflow connections add 'source' --conn-uri 'postgresql://root:root@source-db:5432/source'
	docker exec airflow-webserver airflow connections add 'dest' --conn-uri 'postgresql://root:root@dest-db:5432/dest'
	docker exec tester python setup.py

down:
	docker-compose down

testing:
	docker exec airflow-scheduler airflow dags backfill -s 2019-01-01 sync_source_dest_incr_dag 
	docker exec tester pytest
