<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;

class FirebirdIncrementalTest extends FirebirdBaseTest
{
    private \PDO $connection;

    public function setUp(): void
    {
        parent::setUp();
        $config = $this->getIncrementalConfig();
        $params = $config['parameters']['db'];

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

        $this->connection = new \PDO($dsn, $params['user'], $params['#password'], $options);
    }

    public function testIncrementalFetchingByAutoIncrement(): void
    {
        $config = $this->getIncrementalConfig();
        $this->createAutoIncrementAndTimestampTable($config);
        $this->insertAutoIncrementAndTimestampTableData($config);

        $app = $this->makeApplication($config);
        $result = $app->run();

        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);

        $app = $this->makeApplication($config, $result['state']);
        $emtpyResult = $app->run();
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 1,
            ],
            $emtpyResult['imported']
        );
        $this->insertAutoIncrementAndTimestampTableData($config);

        $app = $this->makeApplication($config, $result['state']);
        $newResult = $app->run();

        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertEquals(4, $newResult['state']['lastFetchedRow']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow']
        );
        $this->assertEquals(3, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingByDecimal(): void
    {
        $config = $this->getIncrementalConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'SOMEDECIMAL';
        $this->createAutoIncrementAndTimestampTable($config);
        $this->insertAutoIncrementAndTimestampTableData($config);

        $result = $this->makeApplication($config)->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);

        $noNewRowsResult = $this->makeApplication($config, $result['state'])->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);
        $this->assertEquals($result['state'], $noNewRowsResult['state']);

        $this->insertAutoIncrementAndTimestampTableData($config, 3);

        $newResult = $this->makeApplication($config, $noNewRowsResult['state'])->run();
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertNotEmpty($newResult['state']['lastFetchedRow']);
        $this->assertGreaterThan($result['state']['lastFetchedRow'], $newResult['state']['lastFetchedRow']);
        $this->assertEquals(4, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingByTimestamp(): void
    {
        $config = $this->getIncrementalConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'sometimestamp';
        $this->createAutoIncrementAndTimestampTable($config);
        $this->insertAutoIncrementAndTimestampTableData($config);

        $result = $this->makeApplication($config)->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);

        $noNewRowsResult = $this->makeApplication($config, $result['state'])->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);
        $this->assertEquals($result['state'], $noNewRowsResult['state']);

        $this->insertAutoIncrementAndTimestampTableData($config, 3);

        $newResult = $this->makeApplication($config, $noNewRowsResult['state'])->run();
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow']
        );
        $this->assertEquals(4, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingByDate(): void
    {
        $config = $this->getIncrementalConfig();
        $config['parameters']['incrementalFetchingColumn'] = 'somedate';
        $this->createAutoIncrementAndTimestampTable($config);
        $this->insertAutoIncrementAndTimestampTableData($config);

        $result = $this->makeApplication($config)->run();

        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );

        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertNotEmpty($result['state']['lastFetchedRow']);

        $noNewRowsResult = $this->makeApplication($config, $result['state'])->run();
        $this->assertEquals(1, $noNewRowsResult['imported']['rows']);
        $this->assertEquals($result['state'], $noNewRowsResult['state']);

        $this->insertAutoIncrementAndTimestampTableData($config, 3);

        $newResult = $this->makeApplication($config, $noNewRowsResult['state'])->run();
        $this->assertArrayHasKey('state', $newResult);
        $this->assertArrayHasKey('lastFetchedRow', $newResult['state']);
        $this->assertGreaterThan(
            $result['state']['lastFetchedRow'],
            $newResult['state']['lastFetchedRow']
        );
        $this->assertEquals(4, $newResult['imported']['rows']);
    }

    public function testIncrementalFetchingLimit(): void
    {
        $config = $this->getIncrementalConfig();
        $config['parameters']['incrementalFetchingLimit'] = 1;
        $this->createAutoIncrementAndTimestampTable($config);
        $this->insertAutoIncrementAndTimestampTableData($config, 10);

        $result = $this->makeApplication($config)->run();
        $this->assertEquals('success', $result['status']);
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 1,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(1, $result['state']['lastFetchedRow']);
        sleep(2);
        // since it's >= we'll set limit to 2 to fetch the second row also
        $config['parameters']['incrementalFetchingLimit'] = 2;
        $result = ($this->makeApplication($config, $result['state']))->run();
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 2,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(2, $result['state']['lastFetchedRow']);

        // test for mega limit
        $config['parameters']['incrementalFetchingLimit'] = 5000;
        $result = ($this->makeApplication($config, $result['state']))->run();
        $this->assertEquals(
            [
                'outputTable' => 'in.c-main.auto-increment-timestamp',
                'rows' => 9,
            ],
            $result['imported']
        );
        //check that output state contains expected information
        $this->assertArrayHasKey('state', $result);
        $this->assertArrayHasKey('lastFetchedRow', $result['state']);
        $this->assertEquals(10, $result['state']['lastFetchedRow']);
    }

    private function createAutoIncrementAndTimestampTable(array $config): void
    {
        $tableExist = $this->connection->query(sprintf(
            'select 1 from rdb$relations where rdb$relation_name = \'%s\'',
            strtoupper($config['parameters']['table']['tableName'])
        ))->fetch();

        if ($tableExist) {
            $tableSql = <<<EOT
DELETE FROM %s;
EOT;
        } else {
            $tableSql = <<<EOT
CREATE TABLE %s (
    id INT, 
    name VARCHAR(255), 
    someDecimal FLOAT,
    someTimestamp TIMESTAMP,
    someDate DATE
);
EOT;
        }

        $this->connection->query(sprintf(
            $tableSql,
            $config['parameters']['table']['tableName']
        ));
    }

    private function getIncrementalConfig(): array
    {
        $config = $this->getConfigRow(self::DRIVER);
        unset($config['parameters']['query']);
        unset($config['parameters']['columns']);
        $config['parameters']['table']['schema'] = null;
        $config['parameters']['table']['tableName'] = 'auto_increment_timestamp';
        $config['parameters']['incremental'] = true;
        $config['parameters']['name'] = 'auto-increment-timestamp';
        $config['parameters']['outputTable'] = 'in.c-main.auto-increment-timestamp';
        $config['parameters']['primaryKey'] = ['id'];
        $config['parameters']['incrementalFetchingColumn'] = 'ID';
        return $config;
    }

    private function insertAutoIncrementAndTimestampTableData(array $config, int $numberRows = 2): void
    {
        $selectSql = sprintf(
            'SELECT MAX(id) as maxId, MAX(somedecimal) as maxdecimal FROM %s',
            $config['parameters']['table']['tableName']
        );
        $maxValues = $this->connection->query($selectSql)->fetch();
        $maxDecimal = (float) $maxValues['MAXDECIMAL'];
        $sqlBaseString = 'INSERT INTO %s VALUES (%d, %s, %s, %s, %s)';
        $dateTime = new \DateTime();
        $dateTime->modify(sprintf('+%s days', (int) $maxValues['MAXID']));
        for ($i = 1; $i <= $numberRows; $i++) {
            $dateTime->modify('+1day');
            $maxDecimal = $this->generateRandomDecimal($maxDecimal);
            $sql = sprintf(
                $sqlBaseString,
                $config['parameters']['table']['tableName'],
                $i+$maxValues['MAXID'],
                $this->quote($this->generateRandomString()),
                $maxDecimal,
                $this->quote('NOW'),
                $this->quote($dateTime->format('Y-m-d'))
            );
            $this->connection->query($sql);
            sleep(1);
        }
    }

    private function generateRandomString(int $length = 10): string
    {
        $includeChars = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
        $charLength = strlen($includeChars);
        $randomString = '';
        for ($i = 0; $i < $length; $i++) {
            $randomString .= $includeChars [rand(0, $charLength - 1)];
        }
        return $randomString;
    }

    private function generateRandomDecimal(float $minimalValue): float
    {
        $maximumValue = 100;
        $randValue = mt_rand(
            (int) ($minimalValue*100000000),
            (int) ($maximumValue*100000000)
        );
        return $randValue/100000000;
    }

    private function quote(string $str): string
    {
        return "'{$str}'";
    }
}
