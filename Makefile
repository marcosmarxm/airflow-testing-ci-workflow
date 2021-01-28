include .env

setup:
	docker-compose up -d --force-recreate --remove-orphans
	sleep 240
	docker exec airflow airflow users create --username admin --password admin --role Admin --firstname Ademir --lastname Junior --email admin@email.com
	docker exec airflow airflow connections add 'oltp' --conn-uri 'postgresql://root:root@oltp-db:5432/oltp'
	docker exec airflow airflow connections add 'olap' --conn-uri 'postgresql://root:root@olap-db:5432/olap'

down:
	docker-compose down

testing:
	docker exec airflow pytest -v
