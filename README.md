# Firebird DB Extractor

This is just a container with Firebird PDO driver. 
Actual PHP application can be found [here](https://github.com/keboola/db-extractor).

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
