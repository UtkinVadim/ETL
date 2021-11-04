# ETL для загрузки данных из Postgres в Elasticsearch

---
1. Запустить контейнер с elasticsearch, если он не запущен:
```console
make run_es
```
---
2. Установить виртуальное окружение и подготовить .env:
```console
make init
source venv/bin/activate
```
---
3. Установить зависимости:
```console
pip install -r requirements.txt
```
---
4. В файле .env настроить переменные под необходимую базу данных.
---
5. Импортировать данные из Postgres в Elasticsearch:
```console
make import
```
---