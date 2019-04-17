<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\UserException;

class Firebird extends Extractor
{
    private const DEFAULT_FIREBIRD_PORT = 3050;

    public function createSshTunnel(array $dbConfig): array
    {
        $connectionParts = explode(':', $dbConfig['dbname']);
        if (count($connectionParts) < 1) {
            throw new UserException('Invalid configuration for ssh tunnel');
        }
        $dbConfig['host'] = $connectionParts[0];
        $dbConfig['port'] = self::DEFAULT_FIREBIRD_PORT;

        return parent::createSshTunnel($dbConfig);
    }

    public function createConnection(array $params): \PDO
    {
        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
        ];

        // check params
        foreach (['dbname', 'user', 'password'] as $r) {
            if (!isset($params[$r])) {
                throw new UserException(sprintf('Parameter %s is missing.', $r));
            }
        }

        $dsn = sprintf(
            'firebird:dbname=%s',
            $params['dbname']
        );

        $pdo = new \PDO($dsn, $params['user'], $params['password'], $options);

        return $pdo;
    }

    public function getConnection(): \PDO
    {
        return $this->db;
    }

    public function testConnection(): void
    {
        $this->db->query('select 1 from rdb$database');
    }

    public function getTables(?array $tables = null): array
    {
        throw new UserException('This component does not yet support the getTables method');
    }

    public function simpleQuery(array $table, array $columns = array()): string
    {
        throw new UserException('This component does not yet support simple queries');
    }
}
