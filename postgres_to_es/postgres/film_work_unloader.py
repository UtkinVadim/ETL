from .base_postgres_unloader import BasePostgresUnloader


class FilmWorkUnloader(BasePostgresUnloader):
    def __init__(self, limit: int = 250):
        super().__init__()
        self.limit = limit
        self.table_name = "filmwork"
        self.state_key = "film_work_updated_at"

    def sql_query(self):
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
        """ % (self.get_updated_at_date(), self.limit)
        return query
