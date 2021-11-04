from typing import List, Union

from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import DictRow
from pydantic import BaseModel

from .backoff import backoff
from .postgres_loader import PostgresLoader


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


class PgToEsLoader:
    def __init__(self, postgres_load_limit: int = 250):
        self.elasticsearch_client = Elasticsearch(hosts="0.0.0.0:9200")
        self.index = "movies"

        self.pg_loader = PostgresLoader(limit=postgres_load_limit)

        self.pg_film_works_data = None
        self.film_work_data_for_transform = None

    @backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def start_process(self) -> None:
        pg_data_generator = self._extract()
        for postgres_film_works_data in pg_data_generator:
            data_for_load = self._transform(postgres_film_works_data)
            self._load(data_for_load)
        self.pg_loader.last_row_number = 0

    def _extract(self):
        pg_data_generator = self.pg_loader.extract_data()
        return pg_data_generator

    def _transform(self, postgres_film_works_data: List[DictRow]) -> List[dict]:
        transformed_film_works_data = []

        for pg_film_work_data in postgres_film_works_data:
            self.film_work_data_for_transform = {**pg_film_work_data}

            self.film_work_data_for_transform.update({"genre": self._get_genre()})

            persons_for_update = self._get_persons_for_update()
            for person_role in persons_for_update:
                self.film_work_data_for_transform.update(
                    {**self._get_persons_info(person_role)}
                )

            validated_film_work_data = FilmWork(**self.film_work_data_for_transform)

            data_for_load = self._transform_data(film_work=validated_film_work_data)

            transformed_film_works_data.append(data_for_load)

        return transformed_film_works_data

    def _get_genre(self) -> list:
        return self.film_work_data_for_transform["genre"].split("|")

    def _get_persons_for_update(self) -> List[str]:
        persons_for_update = []

        actors_in_pg_data = self.film_work_data_for_transform.get("actors", False)
        writers_in_pg_data = self.film_work_data_for_transform.get("writers", False)

        if actors_in_pg_data:
            persons_for_update.append("actors")
        if writers_in_pg_data:
            persons_for_update.append("writers")

        return persons_for_update

    def _get_persons_info(self, persons_role: str) -> dict:
        persons_data = []

        person_names = self.film_work_data_for_transform[f"{persons_role}_names"].split(
            "|"
        )

        persons = self.film_work_data_for_transform[f"{persons_role}"].split("|")
        for person in persons:
            person_id, person_name = person.split(",")[0], person.split(",")[1]
            persons_data.append(Persons(id=person_id, name=person_name))

        return {f"{persons_role}_names": person_names, f"{persons_role}": persons_data}

    def _transform_data(self, film_work: FilmWork) -> dict:
        data_for_load = {"_index": self.index, "_id": film_work.id, **film_work.dict()}
        return data_for_load

    @backoff(start_sleep_time=1, factor=2, border_sleep_time=10)
    def _load(self, film_works_for_load: list) -> None:
        helpers.bulk(self.elasticsearch_client, film_works_for_load)
