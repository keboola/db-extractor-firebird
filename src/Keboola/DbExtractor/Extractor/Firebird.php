<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\DbRetryProxy;
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
        return $retryProxy->call(function () use ($query, $errorMessage): array {
            try {
                $stmt = $this->db->prepare($query);
                $stmt->execute();
                return $stmt->fetchAll(\PDO::FETCH_ASSOC);
            } catch (\Throwable $e) {
                throw new UserException(
                    $errorMessage . ': ' . $e->getMessage(),
                    0,
                    $e
                );
            }
        });
    }
}
