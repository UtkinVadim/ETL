import os
from typing import List

from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import DictRow

from elastic.utils import backoff, get_module_logger

logger = get_module_logger(__name__)


class BaseElasticsearchLoader:
    def __init__(self):
        """
        Базовый класс для загрузки данных в elasticsearch.
        При наследовании от данного класса необходимо указать значения:
            - self.index - индекс, для которого будут добавлены записи в elasticsearch.
            - self.pg_unloader - класс при помощи которого выгружаются данные из postgres.
            - self.model - Модель данных, при помощи которой будет происходить валидация.
            - Также необходимо определить функцию transform_dict_row_to_dict, в которой данные преобразуются из
                DictRow в dict для последующей валидации.
        """
        self.elasticsearch_client = Elasticsearch(hosts=os.environ.get("ES_HOST"))
        self.index = None
        self.pg_unloader = None
        self.model = None

    @backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10)
    def start_process(self) -> None:
        """
        Метод для запуска процесса elastic.
        1. Создание генератора для получения данных таблиц пачками.
        2. Каждая пачка данных трансформируется под данные для загрузки в elasticsearch.
        3. Данные загружаются в elasticsearch.
        """
        pg_data_generator = self.extract()
        for postgres_tables_data in pg_data_generator:
            data_for_load = self.transform(postgres_tables_data)
            self.load(data_for_load)

    def extract(self):
        """
        Метод, возвращающий генератор данных из postgres.
        """
        pg_data_generator = self.pg_unloader.extract_data()
        return pg_data_generator

    def transform(self, postgres_film_works_data: List[DictRow]) -> List[dict]:
        """
        Метод, для преобразования пачки данных полученных из postgres в данные для загрузки в elasticsearch.
        """
        data_for_load = []
        for dict_row in postgres_film_works_data:
            data_for_validate: dict = self.transform_dict_row_to_dict(dict_row)
            model = self.validate_data(data_for_validate)
            transformed_data: dict = self.assign_id_to_data(model)
            data_for_load.append(transformed_data)
        return data_for_load

    def transform_dict_row_to_dict(self, dict_row: DictRow) -> dict:
        """
        Метод, преобразующий сырые данные из postgres в dict для валидации.
        """
        raise NotImplementedError

    def validate_data(self, data_for_validate: dict):
        """
        Метод, для валидации данных.
        """
        try:
            return self.model(**data_for_validate)
        except Exception as err:
            logger.warning(f"{self.model.__name__} validation error: {err}")

    def assign_id_to_data(self, model) -> dict:
        """
        Метод, для присваивания записи id, под которым она будет храниться в elasticsearch.
        """
        data_for_load = {"_id": model.id, **model.dict()}
        return data_for_load

    @backoff(start_sleep_time=1, factor=2, border_sleep_time=10)
    def load(self, data_for_load: list) -> None:
        """
        Метод для загрузки готовых данных в elasticsearch.
        """
        try:
            response = helpers.bulk(self.elasticsearch_client, data_for_load, index=self.index)
            logger.info(f"{self.model.__name__} load success! Loaded row count: {response[0]}.")
            self.pg_unloader.update_state(obj_id=data_for_load[-1].get("_id"))
        except Exception as e:
            logger.warning(f"{self.model.__name__} load error: {e}!")


