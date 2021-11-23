from typing import List, Optional

from pydantic import BaseModel
from psycopg2.extras import DictRow

from elastic.base_elasticsearch_loader import BaseElasticsearchLoader
from elastic.utils import get_module_logger
from postgres.person_unloader import PersonUnloader

logger = get_module_logger(__name__)


class Film(BaseModel):
    id: str
    title: str
    imdb_rating: Optional[float]
    role: str


class Person(BaseModel):
    id: str
    fullname: str
    film_ids: Optional[List[Film]]


class PersonLoader(BaseElasticsearchLoader):
    def __init__(self, postgres_load_limit: int = 250):
        super().__init__()
        self.index = "person"
        self.pg_unloader = PersonUnloader(limit=postgres_load_limit)
        self.model = Person

    def transform_dict_row_to_dict(self, dict_row: DictRow) -> dict:
        return {**dict_row}

