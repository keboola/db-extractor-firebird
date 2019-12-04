<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\Exception\UserException;

class Firebird extends Extractor
{

    public function createConnection(array $params): \PDO
    {
        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
        ];

        // check params
        foreach (['dbname', 'user', '#password'] as $r) {
            if (!isset($params[$r])) {
                throw new UserException(sprintf('Parameter %s is missing.', $r));
            }
        }

        $dsn = sprintf(
            'firebird:dbname=%s',
            $params['dbname']
        );

        $pdo = new \PDO($dsn, $params['user'], $params['#password'], $options);

        return $pdo;
    }

    public function getConnection(): \PDO
    {
        return $this->db;
    }

    public function testConnection(): void
    {
        $this->runRetriableQuery(
            'select 1 from rdb$database',
            'Error test connection'
        );
    }

    public function getTables(?array $tables = null): array
    {
        $resultTables = $this->runRetriableQuery(
            "SELECT TRIM(RDB\$RELATION_NAME) AS NAME,
				CASE RDB\$VIEW_BLR WHEN NULL THEN 'TRUE' ELSE 'FALSE' END AS VIEW_CASE
			FROM RDB\$RELATIONS
			WHERE RDB\$SYSTEM_FLAG is null or RDB\$SYSTEM_FLAG = 0;"
        );
        $tables = [];
        foreach ($resultTables as $table) {
            $tables[] = [
                'name' => $table['NAME'],
                'view' => $table['VIEW_CASE'] === 'TRUE',
                'columns' => $this->getTableColumns($table['NAME']),
            ];
        }
        return $tables;
    }

    private function getTableColumns(string $table): array
    {
        $table = strtoupper($table);
        $resultColumns = $this->runRetriableQuery(
            "SELECT TRIM(r.RDB\$FIELD_NAME) AS FIELD_NAME,
				CASE f.RDB\$FIELD_TYPE
					WHEN 261 THEN 'BLOB'
					WHEN 14 THEN 'CHAR'
					WHEN 40 THEN 'CSTRING'
					WHEN 11 THEN 'D_FLOAT'
					WHEN 27 THEN 'DOUBLE'
					WHEN 10 THEN 'FLOAT'
					WHEN 16 THEN 'INT64'
					WHEN 8 THEN 'INTEGER'
					WHEN 9 THEN 'QUAD'
					WHEN 7 THEN 'SMALLINT'
					WHEN 12 THEN 'DATE'
					WHEN 13 THEN 'TIME'
					WHEN 35 THEN 'TIMESTAMP'
					WHEN 37 THEN 'VARCHAR'
					ELSE 'UNKNOWN'
				END AS FIELD_TYPE,
				f.RDB\$FIELD_LENGTH AS FIELD_LENGTH,
				CASE r.RDB\$NULL_FLAG
					WHEN 1 THEN 'FALSE' ELSE 'TRUE'
				END AS NULLABLE
			FROM RDB\$RELATION_FIELDS r
				LEFT JOIN RDB\$FIELDS f ON r.RDB\$FIELD_SOURCE = f.RDB\$FIELD_NAME
			WHERE r.RDB\$RELATION_NAME = '$table'
			ORDER BY r.RDB\$FIELD_POSITION;"
        );
        $columns = [];
        foreach ($resultColumns as $column) {
            $key = $column['FIELD_NAME'];
            $baseType = new GenericStorage(trim($column['FIELD_TYPE']));
            $columns[] = [
                'name' => $key,
                'type' => $baseType->getBasetype(),
                'length' => $column['FIELD_LENGTH'],
                'nullable' => $column['NULLABLE'] === 'TRUE',
            ];
        }
        return $columns;
    }

    public function simpleQuery(array $table, array $columns = array()): string
    {
        throw new UserException('This component does not yet support simple queries');
    }

    private function runRetriableQuery(string $query, string $errorMessage = ''): array
    {
        $retryProxy = new DbRetryProxy(
            $this->logger,
            DbRetryProxy::DEFAULT_MAX_TRIES,
            [\PDOException::class, \ErrorException::class, \Throwable::class]
        );
        try {
            return $retryProxy->call(function () use ($query): array {
                try {
                    $stmt = $this->db->prepare($query);
                    $stmt->execute();
                    return $stmt->fetchAll(\PDO::FETCH_ASSOC);
                } catch (\Throwable $e) {
                    $this->tryReconnect();
                    throw $e;
                }
            });
        } catch (\Throwable $exception) {
            throw new UserException(
                $errorMessage . ': ' . $exception->getMessage(),
                0,
                $exception
            );
        }
    }

    private function tryReconnect(): void
    {
        try {
            $this->isAlive();
        } catch (DeadConnectionException $deadConnectionException) {
            $reconnectionRetryProxy = new DbRetryProxy($this->logger, self::DEFAULT_MAX_TRIES, null, 1000);
            try {
                $this->db = $reconnectionRetryProxy->call(function () {
                    return $this->createConnection($this->getDbParameters());
                });
            } catch (\Throwable $reconnectException) {
                throw new UserException(
                    'Unable to reconnect to the database: ' . $reconnectException->getMessage(),
                    $reconnectException->getCode(),
                    $reconnectException
                );
            }
        }
    }
}
