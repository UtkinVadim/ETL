from .base_postgres_unloader import BasePostgresUnloader


class PersonUnloader(BasePostgresUnloader):
    def __init__(self, limit: int = 250):
        super().__init__()
        self.limit = limit
        self.table_name = "person"
        self.state_key = "person_updated_at"

    def sql_query(self):
        query = """
                SELECT 
                    person.id, 
                    person.full_name as fullname, 
                    person_film.role, 
                    ARRAY_AGG(DISTINCT jsonb_build_object('id', film.id, 'title', film.title, 'imdb_rating', film.rating))
                FROM content.person person 
                LEFT JOIN content.person_filmwork AS person_film on person.id = person_film.person_id 
                LEFT JOIN content.filmwork AS film on person_film.filmwork_id = film.id 
                WHERE person.updated_at > '%s'
                GROUP BY person.id, person_film.role
                ORDER by person.updated_at
                LIMIT %s;
                """ % (self.get_updated_at_date(), self.limit)
        return query

    def update_state(self, obj_id: str):
        uuid = obj_id.split("_")[0]
        role = obj_id.split("_")[1]
        self.cursor.execute(
            """
            SELECT updated_at
            FROM "content".person AS person
            LEFT JOIN content.person_filmwork AS person_film ON person.id = person_film.person_id
            WHERE person.id = '%s' AND person_film.role = '%s'
            """ % (uuid, role)
        )
        updated_at_date = self.cursor.fetchone()[0]
        self.state.set_state(key=self.state_key, value=str(updated_at_date))
