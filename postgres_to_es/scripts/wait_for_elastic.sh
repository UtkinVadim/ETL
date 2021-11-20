#!/usr/bin/env sh

until nc -z "$ES_HOST" "$ES_PORT"; do
  >&2 echo "Waiting for elastic..."
  sleep 1
done

sh scripts/create_genre_index.sh
sh scripts/create_movies_index.sh
sh scripts/create_person_index.sh

exec "$@"