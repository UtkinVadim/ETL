from typing import List, Optional

from pydantic import BaseModel
from psycopg2.extras import DictRow

from elastic.base_elasticsearch_loader import BaseElasticsearchLoader
from postgres.genre_unloader import GenreUnloader


class Film(BaseModel):
    id: str
    title: str
    imdb_rating: float


class Genre(BaseModel):
    id: str
    name: str
    description: Optional[str]
    film_ids: Optional[List[Film]]


class GenreLoader(BaseElasticsearchLoader):
    def __init__(self, postgres_load_limit: int = 250):
        super().__init__()
        self.index = "genre"
        self.pg_unloader = GenreUnloader(limit=postgres_load_limit)
        self.model = Genre

    def transform_dict_row_to_dict(self, dict_row: DictRow) -> dict:
        return {**dict_row}
