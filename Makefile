run_es:
	docker run -p 9200:9200 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.7.0 -d

create_index_schema:
	sh postgres_to_es/create_index_schema.sh