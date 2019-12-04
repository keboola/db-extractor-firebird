<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

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
        throw new UserException('This component does not yet support the getTables method');
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
