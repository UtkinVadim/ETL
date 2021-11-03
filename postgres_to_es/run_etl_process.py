#!/usr/bin/env python

from postgres_to_es.pg_to_es_loader import PgToEsLoader

if __name__ == "__main__":
    loader = PgToEsLoader(postgres_load_limit=250)
    loader.start_process()
