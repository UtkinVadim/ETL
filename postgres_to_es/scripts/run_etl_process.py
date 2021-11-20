#!/usr/bin/env python
from time import sleep

from etl.pg_to_es_loader import PgToEsLoader
from etl.utils import get_module_logger

loader = PgToEsLoader(postgres_load_limit=250)


if __name__ == "__main__":
    while True:
        try:
            loader.start_process()
        except Exception as err:
            get_module_logger(__name__).warning(err)
        sleep(60)
