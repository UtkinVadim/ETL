1. Установить зависимости и подготовить .env:
```console
make init
```

2. В файле .env настроить переменные под необходимую базу данных.

3. Запустить контейнер с elasticsearch, если он не запущен:
```console
make run_es
```

3. Импортировать данные из Postgres в Elasticsearch:
```console
make init
```