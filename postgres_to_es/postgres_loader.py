import os

from dotenv import find_dotenv, load_dotenv
from psycopg2 import connect
from psycopg2.extras import DictCursor

load_dotenv(find_dotenv(raise_error_if_not_found=False))


class PostgresLoader:
    DSL = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST"),
        "port": os.environ.get("DB_PORT"),
        "options": "-c search_path=content",
    }

    def __init__(self, limit: int = 250, offset: int = 0):
        self.cursor = None
        self.rows_count = None
        self.limit = limit
        self.offset = offset

    def extract_data(self) -> list:
        with connect(**PostgresLoader.DSL, cursor_factory=DictCursor) as pg_connect:
            self.cursor: DictCursor = pg_connect.cursor()
            self._set_rows_count()
            while self.rows_count > 0:
                yield self._get_film_works()

    def _set_rows_count(self):
        self.cursor.execute("""SELECT count(*) FROM "content".filmwork""")
        self.rows_count = self.cursor.fetchone()[0]

    def _get_film_works(self) -> list:
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

            LIMIT %s OFFSET %s;
            """
            % (self.limit, self.offset)
        )
        self.offset += self.limit
        self.rows_count -= self.limit
        return self.cursor.fetchall()
