include .env

setup:
	docker-compose up -d --force-recreate --remove-orphans
	sleep 240
	docker exec airflow airflow users create --username admin --password admin --role Admin --firstname Ademir --lastname Junior --email admin@email.com
	docker exec airflow airflow connections add 'source' --conn-uri 'postgresql://root:root@source-db:5432/source'
	docker exec airflow airflow connections add 'dest' --conn-uri 'postgresql://root:root@dest-db:5432/dest'

down:
	docker-compose down

testing:
	docker exec airflow pytest -vv
