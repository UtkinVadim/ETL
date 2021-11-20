# ETL для загрузки данных из Postgres в Elasticsearch

Для локального использования:
---
1. Запустить контейнер с elasticsearch, если он не запущен:
```console
make run_es_local
```

2. Установить виртуальное окружение и подготовить .env:
```console
make init
source venv/bin/activate
```
3. Установить зависимости:
```console
pip install -r requirements.txt
```
4. В файле .env настроить переменные под необходимую базу данных.
5. Импортировать данные из Postgres в Elasticsearch:
```console
make import
```

Для запуска в контейнере:
---

1. Должен работать контейнер с postgres.
2. Выполнить команду:
```console
docker-compose up
```

Логи контейнера etl можно посмотреть, если запустить:
```console
docker logs -f etl
```