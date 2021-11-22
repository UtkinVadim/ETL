from .base_postgres_unloader import BasePostgresUnloader


class GenreUnloader(BasePostgresUnloader):
    def __init__(self, limit: int = 250):
        super().__init__()
        self.limit = limit
        self.table_name = "genre"
        self.state_key = "genre_updated_at"

    def sql_query(self):
        query = """
                SELECT 
                    genre.id, 
                    genre.name, 
                    genre.description, 
                    ARRAY_AGG(DISTINCT jsonb_build_object('id', film.id, 'title', film.title, 'imdb_rating', film.rating))
                FROM content.genre genre 
                LEFT JOIN content.genre_filmwork as genre_film on genre.id = genre_film.genre_id 
                LEFT JOIN content.filmwork AS film on genre_film.filmwork_id = film.id 
                WHERE genre.updated_at > '%s'
                GROUP BY genre.id
                ORDER by genre.updated_at
                LIMIT %s;
                """ % (self.get_updated_at_date(), self.limit)
        return query
