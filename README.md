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

Additionally, the `dbname` can contain `charset`, `role` and `dialect` [parameters](https://www.php.net/manual/en/ref.pdo-firebird.connection.php).

Valid example database names:

- `my-server:D:\fdb\EMPLOYEE.FDB`
- `my-server:D:\fdb\EMPLOYEE.FDB;role=MY_ROLE`
- `localhost/3050:D:\fdb\EMPLOYEE.FDB`
- `123.123.123.123/3210:/path/to/my.fdb`
- `localhost:/path/to/your/database.fdb;role=MY_ROLE;charset=utf-8`

## License

MIT licensed, see [LICENSE](./LICENSE) file.
