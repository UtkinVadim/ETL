import datetime
import os
from pathlib import Path
from psycopg2 import connect
from psycopg2.extras import DictCursor

from postgres_to_es.postgres.state import JsonFileStorage, State
from postgres_to_es.utils import load_env
from .queries import person_query, film_query

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
            # FIXME это временная заглушка, переписать!
            if 'film' in key:
                data_type = 'filmwork'
            elif 'person' in key:
                data_type = 'person'
            return str(self.start_date(data_type))
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
                while self.rows_count(table_name='filmwork', updated_at=self.get_state_by(key='filmwork_updated_at')) > 0:
                    query = self.generate_query(film_query, self.get_state_by(key='filmwork_updated_at'), self.limit)
                    yield self.get_data_from_db(query)
            elif data_type == 'person':
                while self.rows_count(table_name='person', updated_at=self.get_state_by(key='person_updated_at')) > 0:
                    query = self.generate_query(person_query, self.get_state_by(key='person_updated_at'), self.limit)
                    yield self.get_data_from_db(query)

    def generate_query(self, query: str, state: datetime, limit: int) -> str:
        """
        Формирует и возвращает строку sql запроса для выгрузки фильмов

        :param query:
        :param state:
        :param limit:
        :return:
        """

        return query.format(state, limit)

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

    def start_date(self, data_type: str) -> datetime:
        self.cursor.execute(f"""SELECT fw.updated_at FROM content.{data_type} as fw ORDER BY fw.updated_at limit 1""")
        oldest_row = self.cursor.fetchone()[0] - datetime.timedelta(microseconds=1)
        return oldest_row - datetime.timedelta(microseconds=1)
