#!/usr/bin/env bash


PATH_TO_SQL_DUMP="db.sql"
PATH_TO_ENV=".env"

CONTAINER_NAME=postgres_db
RED="\033[0;31m"
END_COLOR="\033[0m"

echo -e "${RED}###${END_COLOR} Docker ${CONTAINER_NAME} стартует"
docker compose --env-file ${PATH_TO_ENV} up ${CONTAINER_NAME} --build &

echo -e "${RED}###${END_COLOR} Жду пока postgres инициализирует базу"
until docker exec -i ${CONTAINER_NAME} psql -U postgres -c '\q' 2>/dev/null
do
  sleep 5
done

echo -e "${RED}###${END_COLOR} Восстановление дампа"
cat ${PATH_TO_SQL_DUMP} | docker exec -i ${CONTAINER_NAME} psql -U postgres
docker stop ${CONTAINER_NAME}

echo -e "${RED}###${END_COLOR} Контейнер остановлен"
echo -e "${RED}###${END_COLOR} База восстановлена"
echo -e "${RED}###${END_COLOR} Вы великолепны!"
