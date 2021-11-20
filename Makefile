init:
	cp .env.template .env
	python3 -m venv venv

run_es_local:
	docker run -p 9200:9200 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.7.0

import:
	make create_index_schema
	make run_etl_process_local

create_index_schema:
	sh postgres_to_es/scripts/create_movies_index.sh
	sh postgres_to_es/scripts/create_person_index.sh
	sh postgres_to_es/scripts/create_genre_index.sh

run_etl_process_local:
	cd postgres_to_es && python -m scripts.run_etl_process