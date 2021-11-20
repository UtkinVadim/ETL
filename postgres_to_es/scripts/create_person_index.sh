#!/bin/sh

curl -XPUT http://"${ES_HOST}":"${ES_PORT}"/person -H 'Content-Type: application/json' -d'
{
  "settings": {
    "refresh_interval": "1s",
    "analysis": {
      "filter": {
        "english_stop": {
          "type":       "stop",
          "stopwords":  "_english_"
        },
        "english_stemmer": {
          "type": "stemmer",
          "language": "english"
        },
        "english_possessive_stemmer": {
          "type": "stemmer",
          "language": "possessive_english"
        },
        "russian_stop": {
          "type":       "stop",
          "stopwords":  "_russian_"
        },
        "russian_stemmer": {
          "type": "stemmer",
          "language": "russian"
        }
      },
      "analyzer": {
        "ru_en": {
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "english_stop",
            "english_stemmer",
            "english_possessive_stemmer",
            "russian_stop",
            "russian_stemmer"
          ]
        }
      }
    }
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "id": {
        "type": "keyword"
      },
      "fullname": {
        "type": "text",
        "analyzer": "ru_en"
      },
      "role": {
        "type": "keyword"
      },
      "film_ids": {
        "type": "nested",
        "dynamic": "strict",
        "properties": {
          "id": {
            "type": "keyword"
          },
          "title": {
            "type": "text",
            "analyzer": "ru_en"
          },
          "imdb_rating": {
            "type": "float"
          }
        }
      }
    }
  }
}'