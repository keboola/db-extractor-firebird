<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/02/16
 * Time: 17:49
 */

namespace Keboola\DbExtractor\Extractor;

use Keboola\DbExtractor\Exception\UserException;

class Firebird extends Extractor
{
    public function createSshTunnel($dbConfig)
    {
        if (!isset($dbConfig['host'])) {
            $dbConfig['host'] = $dbConfig['dbname'];
            $dbConfig['port'] = 3050;
        }
        return parent::createSshTunnel($dbConfig);
    }

    public function createConnection($params)
    {
        // convert errors to PDOExceptions
        $options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC
        ];

        // check params
        foreach (['dbname', 'user', 'password'] as $r) {
            if (!isset($params[$r])) {
                throw new UserException(sprintf("Parameter %s is missing.", $r));
            }
        }

        $dsn = sprintf(
            "firebird:dbname=%s",
            $params['dbname']
        );

        $pdo = new \PDO($dsn, $params['user'], $params['password'], $options);

        return $pdo;
    }

    public function getConnection()
    {
        return $this->db;
    }

    public function testConnection()
    {
        $this->db->query('select 1 from rdb$database');
    }
}
