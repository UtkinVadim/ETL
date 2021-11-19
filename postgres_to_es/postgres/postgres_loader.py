import datetime
import os
from pathlib import Path
from psycopg2 import connect
from psycopg2.extras import DictCursor

from postgres_to_es.postgres.state import JsonFileStorage, State
from postgres_to_es.utils import load_env

FILE_PATH = Path(__file__).resolve().parent

load_env()


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
        self.limit = limit

        self.cursor = None

        self.storage = JsonFileStorage(file_path=str(FILE_PATH / "state.json"))
        self.state = State(self.storage)

    def get_state_by(self, key: str) -> datetime:
        """
        Возвращает сохранённое значение по ключу key

        :param key:
        :return:
        """
        state = self.state.get_state(key=key)
        if state is None:
            return str(self.start_date())
        return state

    def rows_count(self, table_name: str, updated_at: str) -> int:
        """
        Делает запрос базе и возвращает кол-во строк в таблице дата обновления которых старше updated_at

        :param table_name:
        :param updated_at:
        :return:
        """
        self.cursor.execute(
            f"""SELECT count(*)
               FROM "content".{table_name}
               WHERE updated_at > '{updated_at}'"""
        )
        return self.cursor.fetchone()[0]

    def extract_data(self, data_type: str) -> list:
        with connect(**PostgresLoader.DSL, cursor_factory=DictCursor) as pg_connect:
            self.cursor: DictCursor = pg_connect.cursor()
            if data_type == 'filmwork':
                while self.rows_count(table_name='filmwork', updated_at=self.get_state_by(key='film_updated_at')) > 0:
                    yield self.get_data_from_db(self.make_film_query())
            elif data_type == 'person':
                while self.rows_count(table_name='person', updated_at=self.get_state_by(key='persons_updated_at')) > 0:
                    yield self.get_data_from_db(self.make_person_query())

    def make_film_query(self) -> str:
        """
        Формирует и возвращает строку sql запроса для выгрузки фильмов

        :return:
        """
        query = """
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

        WHERE updated_at > '%s'

        ORDER BY fw.updated_at

        LIMIT %s;
        """ % (self.get_state_by(key='film_updated_at'), self.limit)
        return query

    def make_person_query(self) -> str:
        """
        Формирует и возвращает строку sql запроса для выгрузки персон

        :return:
        """
        query = """
        SELECT 
            person.id, 
            person.full_name as fullname, 
            person_film.role, 
            ARRAY_AGG(DISTINCT jsonb_build_object('id', film.id, 'title', film.title, 'imdb_rating', film.rating))
        FROM content.person person 
        LEFT JOIN content.person_filmwork as person_film on person.id = person_film.person_id 
        LEFT JOIN content.filmwork AS film on person_film.filmwork_id = film.id 
        WHERE person.updated_at > '%s'
        GROUP BY person.id, person_film.role
        ORDER by person.updated_at
        LIMIT %s;
        """ % (self.get_state_by(key='persons_updated_at'), self.limit)
        return query

    def get_data_from_db(self, query) -> list:
        """
        Делает запрос query к базе, возвращает полученные данные

        :return:
        """
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def update_state(self, data_type: str, key: str, value: str):
        """
        Обновляет состояние, данные о состоянии берёт из базы.

        :param data_type:
        :param key:
        :param value:
        :return:
        """
        self.cursor.execute(
            f"""SELECT updated_at
               FROM "content".{data_type}
               WHERE id = '{value}'"""
        )
        updated_at_date = self.cursor.fetchone()[0]
        self.state.set_state(key=key, value=str(updated_at_date))

    def start_date(self):
        self.cursor.execute("""SELECT fw.updated_at FROM content.filmwork as fw ORDER BY fw.updated_at limit 1""")
        oldest_row = self.cursor.fetchone()[0] - datetime.timedelta(microseconds=1)
        return oldest_row - datetime.timedelta(microseconds=1)
