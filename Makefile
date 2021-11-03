init:
	cp .env.tamplate .env
	pip install -r requirements.txt

run_es:
	docker run -p 9200:9200 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.7.0

import:
	make create_index_schema
	make run_etl_process

create_index_schema:
	sh postgres_to_es/create_index_schema.sh

run_etl_process:
	python -m postgres_to_es.run_etl_process