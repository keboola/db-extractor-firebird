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

//        $port = isset($params['port']) ? $params['port'] : '21050';

        $dsn = sprintf(
            "firebird:dbname=%s",
            $params['dbname']
        );

        $pdo = new \PDO($dsn, $params['user'], $params['password'], $options);
//        $pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);

        return $pdo;
    }

    public function getConnection()
    {
        return $this->db;
    }

}