import os
from pathlib import Path

from psycopg2 import connect
from psycopg2.extras import DictCursor

from postgres_to_es.postgres.state import JsonFileStorage, State

FILE_PATH = Path(__file__).resolve().parent


class PostgresLoader:
    DSL = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST"),
        "port": os.environ.get("DB_PORT"),
        "options": "-c search_path=content",
    }

    def __init__(self, limit: int = 250):
        self.cursor = None
        self.rows_left = None
        self.limit = limit

        self.storage = JsonFileStorage(file_path=str(FILE_PATH / "state.json"))
        self.state = State(self.storage)

    @property
    def rows_count(self) -> int:
        self.cursor.execute("""SELECT count(*) FROM "content".filmwork""")
        return self.cursor.fetchone()[0]

    def extract_data(self) -> list:
        with connect(**PostgresLoader.DSL, cursor_factory=DictCursor) as pg_connect:
            self.cursor: DictCursor = pg_connect.cursor()
            self.state.set_state(key="last_row_number", value=0)
            self.rows_left = self.rows_count
            while self.rows_left > 0:
                yield self.get_film_works()

    def get_film_works(self) -> list:
        last_row_number = self.state.get_state(key="last_row_number")

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
            SELECT pfw.filmwork_id,
                   pfw.role,
                   string_agg(p.full_name, '|') AS perons,
                   string_agg(concat(p.id, ',' ,p.full_name), '|') AS id_names
            FROM "content".person_filmwork pfw
            JOIN "content".person p ON p.id = pfw.person_id
            GROUP BY pfw.filmwork_id, pfw.role
            )

            SELECT fw.id,
                   COALESCE(fw.rating, 0.0) AS imdb_rating,
                   cg.genre,
                   fw.title,
                   fw.description,
                   cpd.perons AS director,
                   cpa.perons AS actors_names,
                   cpw.perons AS writers_names,
                   cpa.id_names AS actors,
                   cpw.id_names AS writers

            FROM "content".filmwork fw

            LEFT OUTER JOIN cte_genres cg ON cg.filmwork_id = fw.id
            LEFT OUTER JOIN cte_persons cpa ON cpa.filmwork_id = fw.id AND cpa.role = 'actor'
            LEFT OUTER JOIN cte_persons cpd ON cpd.filmwork_id = fw.id AND cpd.role = 'director'
            LEFT OUTER JOIN cte_persons cpw ON cpw.filmwork_id = fw.id AND cpw.role = 'writer'
            ORDER BY fw.id

            LIMIT %s OFFSET %s;
            """
            % (self.limit, last_row_number if last_row_number else 0)
        )
        self.state.set_state(key="last_row_number", value=last_row_number + self.limit)
        self.rows_left -= self.limit
        return self.cursor.fetchall()
