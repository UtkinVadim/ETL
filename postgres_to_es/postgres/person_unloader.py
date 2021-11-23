from .base_postgres_unloader import BasePostgresUnloader


class PersonUnloader(BasePostgresUnloader):
    def __init__(self, limit: int = 250):
        super().__init__()
        self.limit = limit
        self.table_name = "person"
        self.state_key = "person_updated_at"

    def sql_query(self):
        query = """
                WITH role AS (SELECT person_film.role
                       FROM content.person_filmwork AS person_film
                                LEFT JOIN content.filmwork AS film on person_film.filmwork_id = film.id
                )
                SELECT person.id,
                       person.full_name AS fullname,
                       ARRAY_AGG(DISTINCT jsonb_build_object(
                                    'id', film.id,
                                    'title', film.title,
                                    'imdb_rating', film.rating,
                                    'role', role
                           )) as film_ids
                FROM content.person person
                         LEFT JOIN content.person_filmwork AS person_film on person.id = person_film.person_id
                         LEFT JOIN content.filmwork AS film on person_film.filmwork_id = film.id
                                WHERE person.updated_at > '%s'
                GROUP BY person.id
                ORDER by person.updated_at  
                LIMIT %s;
                """ % (self.get_updated_at_date(), self.limit)
        return query

    def update_state(self, obj_id: str):
        self.cursor.execute(
            """
            SELECT updated_at
            FROM "content".person AS person
            LEFT JOIN content.person_filmwork AS person_film ON person.id = person_film.person_id
            WHERE person.id = '%s'
            """ % obj_id
        )
        updated_at_date = self.cursor.fetchone()[0]
        self.state.set_state(key=self.state_key, value=str(updated_at_date))
