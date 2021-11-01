import os
from typing import List, Union

from psycopg2 import connect
from psycopg2.extras import DictCursor

from dotenv import find_dotenv, load_dotenv
from pydantic import BaseModel
from pprint import pprint
load_dotenv(find_dotenv(raise_error_if_not_found=False))

DSL = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT"),
    "options": "-c search_path=content",
}


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


class PostgresLoader:
    def __init__(self, connection: connect):
        self.cursor: DictCursor = connection.cursor()

    def get_film_works(self):
        """
        Метод для получения информации о фильме..
        """
        self.cursor.execute(
            """
            WITH

            cte_genres AS (
            SELECT gfw.filmwork_id, string_agg(NAME, '|') AS genre
            FROM "content".genre_filmwork gfw
            JOIN "content".genre g ON g.id = gfw.genre_id
            GROUP BY gfw.filmwork_id
            ),

            cte_persons AS (
            SELECT pfw.filmwork_id, pfw.role, string_agg(p.full_name, '|') AS perons
            FROM "content".person_filmwork pfw
            JOIN "content".person p ON p.id = pfw.person_id
            GROUP BY pfw.filmwork_id, pfw.role
            ),
            
            cte_id_names AS (
            SELECT pfw.filmwork_id, pfw.role, string_agg(concat(p.id, ',' ,p.full_name), '|') AS id_names
            FROM "content".person_filmwork pfw
            JOIN "content".person p ON p.id = pfw.person_id
            GROUP BY pfw.filmwork_id, pfw.role
            )

            SELECT fw.id,
                   fw.rating AS imdb_rating,
                   cg.genre,
                   fw.title,
                   fw.description,
                   cpd.perons AS director,
                   cpa.perons AS actors_names,
                   cpw.perons AS writers_names,
                   actors.id_names AS actors,
                   writers.id_names AS writers

            FROM "content".filmwork fw

            LEFT OUTER JOIN cte_genres cg ON cg.filmwork_id = fw.id
            LEFT OUTER JOIN cte_persons cpa ON cpa.filmwork_id = fw.id AND cpa.role = 'actor'
            LEFT OUTER JOIN cte_persons cpd ON cpd.filmwork_id = fw.id AND cpd.role = 'director'
            LEFT OUTER JOIN cte_persons cpw ON cpw.filmwork_id = fw.id AND cpw.role = 'writer'
            LEFT OUTER JOIN cte_id_names as actors ON actors.filmwork_id = fw.id AND actors.role = 'actor'
            LEFT OUTER JOIN cte_id_names as writers ON writers.filmwork_id = fw.id AND writers.role = 'writer'

            LIMIT 500;
            """
        )

        film_works = []
        for film_work_data in self.cursor.fetchall():
            data = {**film_work_data}

            data["genre"] = data.get("genre").split("|")

            if actors_names := data.get("actors_names"):
                data["actors_names"] = actors_names.split("|")
                actors_data = data["actors"].split("|")
                actors = []
                for actor in actors_data:
                    actor_info = actor.split(",")
                    actors.append(Persons(id=actor_info[0], name=actor_info[1]))
                data["actors"] = actors

            if writers_names := data.get("writers_names"):
                data["writers_names"] = writers_names.split("|")
                writers_data = data["writers"].split("|")
                writers = []
                for writer in writers_data:
                    writer_info = writer.split(",")
                    writers.append(Persons(id=writer_info[0], name=writer_info[1]))
                data["writers"] = actors

            film_works.append(FilmWork(**data))
        pprint(film_works)
        return


if __name__ == "__main__":
    with connect(**DSL, cursor_factory=DictCursor) as pg_connect:
        loader = PostgresLoader(connection=pg_connect)
        loader.get_film_works()
