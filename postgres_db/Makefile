.PHONY: cp_env, fill_database, run, first_run
cp_env:
	cp .env.template .env
fill_database:
	bash 01_init_db.sh
run:
	docker compose --env-file .env up -d
first_run: cp_env fill_database run