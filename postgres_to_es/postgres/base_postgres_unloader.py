import os
import datetime
from pathlib import Path

from psycopg2 import connect
from psycopg2.extras import DictCursor

from postgres.state import JsonFileStorage, State
from elastic.utils import load_env

FILE_PATH = Path(__file__).resolve().parent
THE_BEGINNING_OF_TIME = "1970-01-01 00:00:00"

load_env()

DSL = {
        "dbname": os.environ.get("POSTGRES_DB"),
        "user": os.environ.get("POSTGRES_USER"),
        "password": os.environ.get("POSTGRES_PASSWORD"),
        "host": os.environ.get("POSTGRES_HOST"),
        "port": os.environ.get("POSTGRES_PORT"),
        "options": "-c search_path=content",
    }


class BasePostgresUnloader:
    def __init__(self):
        """
        Базовый класс для выгрузки данных из postgres.
        При наследовании от данного класса необходимо указать значения:
            - self.limit - число, обозначающее количество записываемых данных в пачке.
            - self.table - имя таблицы из которой выгружаем данные.
            - self.state_key - имя ключа, по которому будут записываться и извлекаться данные из файла state.json.
            - Также необходимо определить функцию sql_query, в которой описать sql код для получения данных из postgres.
        """
        self.storage = JsonFileStorage(file_path=str(FILE_PATH / "state.json"))
        self.state = State(self.storage)
        self.limit = None
        self.table_name = None
        self.state_key = None

        self.cursor = None
        self.rows_left = None

    def sql_query(self):
        """
        SQL запрос для получения данных.
        """
        raise NotImplemented

    def extract_data(self) -> list:
        """
        Метод возвращает генератор, возвращающий записи пачками в количестве, указанном в self.limit атрибуте.
        """
        with connect(**DSL, cursor_factory=DictCursor) as pg_connect:
            self.cursor: DictCursor = pg_connect.cursor()
            while self.get_rows_count() > 0:
                yield self.get_table_data()

    def get_table_data(self) -> list:
        """
        Метод, достающий пачку данных из postgres.
        """
        self.cursor.execute(self.sql_query())
        return self.cursor.fetchall()

    def get_rows_count(self) -> int:
        """
        Метод, возвращающий оставшееся количество строк для записи, начиная от последней сохраненной записи.
        """
        self.cursor.execute(
            """
            SELECT count(*) FROM "content".%s WHERE updated_at > '%s'
            """ % (self.table_name, self.get_updated_at_date())
        )
        return self.cursor.fetchone()[0]

    def get_updated_at_date(self) -> datetime:
        """
        Метод, для получения datetime последней загруженной в elasticsearch записи.
        """
        try:
            state_value = self.state.get_state(key=self.state_key)
        except FileNotFoundError:
            return THE_BEGINNING_OF_TIME
        else:
            return state_value if state_value else THE_BEGINNING_OF_TIME

    def update_state(self, obj_id: str):
        """
        Метод для записи нового значения update_at в State.
        """
        self.cursor.execute(
            """
            SELECT updated_at FROM "content".%s WHERE id = '%s'
            """ % (self.table_name, obj_id)
        )
        updated_at_date = self.cursor.fetchone()[0]
        self.state.set_state(key=self.state_key, value=str(updated_at_date))
