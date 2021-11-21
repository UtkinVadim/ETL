from typing import List, Union

from pydantic import BaseModel
from psycopg2.extras import DictRow

from elastic.base_elasticsearch_loader import BaseElasticsearchLoader
from postgres.film_work_unloader import FilmWorkUnloader


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


class FilmWorkLoader(BaseElasticsearchLoader):
    def __init__(self, postgres_load_limit: int = 250):
        super().__init__()
        self.index = "movies"
        self.pg_unloader = FilmWorkUnloader(limit=postgres_load_limit)
        self.model = FilmWork

    def transform_dict_row_to_dict(self, dict_row: DictRow) -> dict:
        film_work_data = {**dict_row}
        film_work_data.update({"genre": self.get_genre(film_work_data)})
        persons_for_update = self.get_persons_for_update(film_work_data)
        for person_role in persons_for_update:
            film_work_data.update(
                {**self.get_persons_info(person_role, film_work_data)}
            )
        return film_work_data

    def get_genre(self, film_work_data: dict) -> list:
        return film_work_data["genre"].split("|")

    def get_persons_for_update(self, film_work_data: dict) -> List[str]:
        persons_for_update = []

        actors_in_pg_data = film_work_data.get("actors", False)
        writers_in_pg_data = film_work_data.get("writers", False)

        if actors_in_pg_data:
            persons_for_update.append("actors")
        if writers_in_pg_data:
            persons_for_update.append("writers")

        return persons_for_update

    def get_persons_info(self, persons_role: str, film_work_data: dict) -> dict:
        persons_data = []

        person_names = film_work_data[f"{persons_role}_names"].split(
            "|"
        )

        persons = film_work_data[f"{persons_role}"].split("|")
        for person in persons:
            person_id, person_name = person.split(",")[0], person.split(",")[1]
            persons_data.append(Persons(id=person_id, name=person_name))

        return {f"{persons_role}_names": person_names, f"{persons_role}": persons_data}
