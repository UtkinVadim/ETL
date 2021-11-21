FROM python:3.9.7-alpine
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1

COPY postgres_to_es .

RUN apk update && pip install -U pip
RUN apk add --virtual .build-deps gcc python3-dev musl-dev postgresql-dev linux-headers libffi-dev \
     jpeg-dev zlib-dev curl bash

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN addgroup -S etl_user && \
    adduser -S -H -G etl_user etl_user && \
    chown -R etl_user:etl_user /app

USER etl_user

ENTRYPOINT ["bash", "./scripts/wait_for_elastic.sh"]

CMD python -m scripts.run_etl_process