# Firebird DB Extractor


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
