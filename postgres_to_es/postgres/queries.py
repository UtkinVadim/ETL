person_query = """
    SELECT 
        person.id, 
        person.full_name as fullname, 
        person_film.role, 
        ARRAY_AGG(DISTINCT jsonb_build_object('id', film.id, 'title', film.title, 'imdb_rating', film.rating))
    FROM content.person person 
    LEFT JOIN content.person_filmwork AS person_film on person.id = person_film.person_id 
    LEFT JOIN content.filmwork AS film on person_film.filmwork_id = film.id 
    WHERE person.updated_at > '{}'
    GROUP BY person.id, person_film.role
    ORDER by person.updated_at
    LIMIT {};
    """

genre_query = """
    SELECT 
        genre.id, 
        genre.name, 
        genre.description, 
        ARRAY_AGG(DISTINCT jsonb_build_object('id', film.id, 'title', film.title, 'imdb_rating', film.rating))
    FROM content.genre genre 
    LEFT JOIN content.genre_filmwork as genre_film on genre.id = genre_film.genre_id 
    LEFT JOIN content.filmwork AS film on genre_film.filmwork_id = film.id 
    WHERE genre.updated_at > '{}'
    GROUP BY genre.id
    ORDER by genre.updated_at
    LIMIT {};
    """


film_query = """       
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

        WHERE updated_at > '{}'

        ORDER BY fw.updated_at

        LIMIT {};"""