<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\GenericStorage;
use Keboola\DbExtractor\DbRetryProxy;
use Keboola\DbExtractor\Exception\DeadConnectionException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\TableResultFormat\Table;
use Keboola\DbExtractor\TableResultFormat\TableColumn;

class Firebird extends Extractor
{
    public const INCREMENT_TYPE_NUMERIC = 'numeric';
    public const INCREMENT_TYPE_TIMESTAMP = 'timestamp';
    public const INCREMENT_TYPE_DATE = 'date';
    public const NUMERIC_BASE_TYPES = ['INTEGER', 'NUMERIC', 'FLOAT'];

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
        $dbName = $params['dbname'];
        if (isset($params['ssh']['enabled']) && $params['ssh']['enabled']) {
            preg_match('/([^\/]+)\/?([0-9]+)?:(.*)/', $params['dbname'], $connectionParts);
            $dbName = sprintf('%s/%s:%s', $params['host'], $params['port'], $connectionParts[3]);
        }

        $dsn = sprintf('firebird:dbname=%s', $dbName);

        $this->logger->info(sprintf('Connecting to "%s"', $dsn));
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

    public function validateIncrementalFetching(array $table, string $columnName, ?int $limit = null): void
    {
        $columnName = strtoupper($columnName);
        $column = $this->getTableColumns($table['tableName'], $columnName);
        array_walk($column, function (TableColumn &$item): void {
            $item = $item->getOutput();
        });

        try {
            $datatype = new GenericStorage($column[0]['type']);
            if (in_array($datatype->getBasetype(), self::NUMERIC_BASE_TYPES)) {
                $this->incrementalFetching['column'] = $columnName;
                $this->incrementalFetching['type'] = self::INCREMENT_TYPE_NUMERIC;
            } elseif ($datatype->getBasetype() === 'TIMESTAMP') {
                $this->incrementalFetching['column'] = $columnName;
                $this->incrementalFetching['type'] = self::INCREMENT_TYPE_TIMESTAMP;
            } elseif ($datatype->getBasetype() === 'DATE') {
                $this->incrementalFetching['column'] = $columnName;
                $this->incrementalFetching['type'] = self::INCREMENT_TYPE_DATE;
            } else {
                throw new UserException('invalid incremental fetching column type');
            }
        } catch (InvalidLengthException | UserException $exception) {
            throw new UserException(
                sprintf(
                    'Column [%s] specified for incremental fetching is not a numeric or timestamp type column',
                    $columnName
                )
            );
        }
        if ($limit) {
            $this->incrementalFetching['limit'] = $limit;
        }
    }

    public function getMaxOfIncrementalFetchingColumn(array $table): ?string
    {
        $fullsql = sprintf(
            'SELECT MAX(%s) as %s FROM %s %s',
            $this->incrementalFetching['column'],
            $this->incrementalFetching['column'],
            $table['tableName'],
            $this->getIncrementalQueryAddon()
        );
        $result = $this->runRetriableQuery($fullsql, 'Fetching incremental max value error');
        if (count($result) > 0) {
            return (string) $result[0][$this->incrementalFetching['column']];
        }
        return null;
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
            $outputTable = new Table();
            $outputTable
                ->setSchema('')
                ->setName($table['NAME'])
                ->setType($table['VIEW_CASE'] === 'TRUE' ? 'view' : 'table')
                ->setColumns($this->getTableColumns($table['NAME']));

            $tables[] = $outputTable->getOutput();
        }
        return $tables;
    }

    private function getTableColumns(string $table, ?string $columnName = null): array
    {
        $table = strtoupper($table);
        $sqlColumns =
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
			WHERE r.RDB\$RELATION_NAME = '$table'";

        if (!is_null($columnName)) {
            $sqlColumns .= " AND TRIM(r.RDB\$FIELD_NAME) = '$columnName'";
        }
        $sqlColumns .= ' ORDER BY r.RDB$FIELD_POSITION;';
        $resultColumns = $this->runRetriableQuery($sqlColumns);
        $columns = [];
        foreach ($resultColumns as $column) {
            $baseType = new GenericStorage(
                trim($column['FIELD_TYPE']),
                ['length' => $column['FIELD_LENGTH']]
            );
            $tableColumn = new TableColumn();
            $tableColumn
                ->setName((string) $column['FIELD_NAME'])
                ->setType((string) $baseType->getBasetype())
                ->setLength((string) $baseType->getLength())
                ->setNullable($column['NULLABLE'] === 'TRUE');
            $columns[] = $tableColumn;
        }
        return $columns;
    }

    public function simpleQuery(array $table, array $columns = array()): string
    {
        if (count($columns) > 0) {
            $query = sprintf(
                'SELECT %s %s FROM %s',
                implode(', ', array_map(function ($column): string {
                    return $column;
                }, $columns)),
                $this->getIncrementalLimitAddon(),
                $table['tableName']
            );
        } else {
            $query = sprintf(
                'SELECT %s * FROM %s',
                $this->getIncrementalLimitAddon(),
                $table['tableName']
            );
        }

        $incrementalAddon = $this->getIncrementalQueryAddon();
        if ($incrementalAddon) {
            $query .= $incrementalAddon;
        }
        if ($this->hasIncrementalLimit()) {
            $query .= sprintf(
                ' ORDER BY %s',
                $this->incrementalFetching['column']
            );
        }

        return $query;
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

    private function getIncrementalQueryAddon(): ?string
    {
        $incrementalAddon = null;
        if ($this->incrementalFetching) {
            if (isset($this->state['lastFetchedRow'])) {
                $incrementalAddon = sprintf(
                    ' WHERE %s >= %s',
                    $this->incrementalFetching['column'],
                    $this->shouldQuoteComparison($this->incrementalFetching['type'])
                        ? $this->db->quote($this->state['lastFetchedRow'])
                        : $this->state['lastFetchedRow']
                );
            }
        }
        return $incrementalAddon;
    }

    private function getIncrementalLimitAddon(): string
    {
        $limitAddon = '';
        if ($this->hasIncrementalLimit()) {
            $limitAddon .= sprintf('FIRST %d', $this->incrementalFetching['limit']);
        }
        return $limitAddon;
    }

    private function shouldQuoteComparison(string $type): bool
    {
        if ($type === self::INCREMENT_TYPE_NUMERIC) {
            return false;
        }
        return true;
    }
}
