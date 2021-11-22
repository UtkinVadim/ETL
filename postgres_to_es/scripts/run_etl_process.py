#!/usr/bin/env python
from time import sleep

from elastic.film_work_loader import FilmWorkLoader
from elastic.genre_loader import GenreLoader
from elastic.person_loader import PersonLoader
from elastic.utils import get_module_logger


if __name__ == "__main__":
    while True:
        try:
            FilmWorkLoader(postgres_load_limit=500).start_process()
            GenreLoader(postgres_load_limit=500).start_process()
            PersonLoader(postgres_load_limit=2000).start_process()
        except Exception as err:
            get_module_logger(__name__).warning(err)
        sleep(60)
