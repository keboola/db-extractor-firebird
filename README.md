# Firebird DB Extractor

[![Build Status](https://travis-ci.org/keboola/db-extractor-firebird.svg?branch=master)](https://travis-ci.org/keboola/db-extractor-firebird)


## Example configuration


    {
      "db": {
        "driver": "firebird",
        "dbname": "localhost:/path/to/your/database.fdb",
        "user": "USERNAME",
        "password": "PASSWORD"
      },
      "tables": [
        {
          "name": "tables",
          "query": "SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG = 0",
          "outputTable": "in.c-main.tables",
          "incremental": false,
          "enabled": true,
          "primaryKey": null
        }
      ]
    }
