import os
from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import DictRow
from pydantic import BaseModel
from typing import List, Union, Optional
from uuid import UUID

from postgres_to_es.postgres.postgres_loader import PostgresLoader
from postgres_to_es.utils import backoff, get_logger


class Persons(BaseModel):
    id: str
    name: str


class FilmWork(BaseModel):
    id: str
    imdb_rating: float
    genre: List[str]
    title: str
    description: Union[str, None]
    director: Union[str, None]
    actors_names: Union[List[str], None]
    writers_names: Union[List[str], None]
    actors: Union[List[Persons], None]
    writers: Union[List[Persons], None]


class Film(BaseModel):
    id: Union[str, UUID]
    title: str
    imdb_rating: float


class PersonWithFilms(BaseModel):
    id: Union[str, UUID]
    fullname: str
    role: str
    film_ids: Optional[List[Film]]


class PgToEsLoader:
    def __init__(self, postgres_load_limit: int = 250):
        self.elasticsearch_client = Elasticsearch(hosts=os.environ.get("ES_HOST"))
        self.index = "movies"

        self.pg_loader = PostgresLoader(limit=postgres_load_limit)

        self.pg_film_works_data = None
        self.film_work_data_for_transform = None

    @backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def start_process(self) -> None:
        pg_data_generator = self.extract()
        for postgres_film_works_data in pg_data_generator:
            data_for_load = self.transform(postgres_film_works_data)
            self.load(data_for_load)

    def extract(self):
        pg_data_generator = self.pg_loader.extract_data()
        return pg_data_generator

    def transform(self, postgres_film_works_data: List[DictRow]) -> List[dict]:
        transformed_film_works_data = []

        for pg_film_work_data in postgres_film_works_data:
            self.film_work_data_for_transform = {**pg_film_work_data}

            self.film_work_data_for_transform.update({"genre": self.get_genre()})

            persons_for_update = self.get_persons_for_update()
            for person_role in persons_for_update:
                self.film_work_data_for_transform.update(
                    {**self.get_persons_info(person_role)}
                )

            validated_film_work_data = self.validate_film_work_data()

            data_for_load = self.transform_data(film_work=validated_film_work_data)

            transformed_film_works_data.append(data_for_load)

        return transformed_film_works_data

    def validate_film_work_data(self) -> FilmWork:
        try:
            return FilmWork(**self.film_work_data_for_transform)
        except Exception as err:
            get_logger(__name__).warning(f"Film work validation error: {err}")

    def get_genre(self) -> list:
        return self.film_work_data_for_transform["genre"].split("|")

    def get_persons_for_update(self) -> List[str]:
        persons_for_update = []

        actors_in_pg_data = self.film_work_data_for_transform.get("actors", False)
        writers_in_pg_data = self.film_work_data_for_transform.get("writers", False)

        if actors_in_pg_data:
            persons_for_update.append("actors")
        if writers_in_pg_data:
            persons_for_update.append("writers")

        return persons_for_update

    def get_persons_info(self, persons_role: str) -> dict:
        persons_data = []

        person_names = self.film_work_data_for_transform[f"{persons_role}_names"].split(
            "|"
        )

        persons = self.film_work_data_for_transform[f"{persons_role}"].split("|")
        for person in persons:
            person_id, person_name = person.split(",")[0], person.split(",")[1]
            persons_data.append(Persons(id=person_id, name=person_name))

        return {f"{persons_role}_names": person_names, f"{persons_role}": persons_data}

    def transform_data(self, film_work: FilmWork) -> dict:
        data_for_load = {"_index": self.index, "_id": film_work.id, **film_work.dict()}
        return data_for_load

    @backoff(start_sleep_time=1, factor=2, border_sleep_time=10)
    def load(self, film_works_for_load: list) -> None:
        helpers.bulk(self.elasticsearch_client, film_works_for_load)
        self.pg_loader.update_state(film_work_id=film_works_for_load[-1].get("_id"))
