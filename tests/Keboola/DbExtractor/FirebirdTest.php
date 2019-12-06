<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Extractor\Firebird;
use Keboola\DbExtractorLogger\Logger;

class FirebirdTest extends FirebirdBaseTest
{
    public function testRun(): void
    {
        $result = ($this->makeApplication($this->getConfig(self::DRIVER)))->run();
        $expectedCsvFile = $this->dataDir . '/firebird/' . $result['imported'][0]['outputTable'] . '.csv';
        $expectedManifestFile = $this->dataDir . '/firebird/' . $result['imported'][0]['outputTable'] . '.csv.manifest';
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
        $this->assertEquals(file_get_contents($expectedManifestFile), file_get_contents($outputManifestFile));
    }

    public function testRowRun(): void
    {
        $result = ($this->makeApplication($this->getConfigRow(self::DRIVER)))->run();
        $expectedCsvFile = $this->dataDir . '/firebird/' . $result['imported']['outputTable'] . '.csv';
        $expectedManifestFile = $this->dataDir . '/firebird/' . $result['imported']['outputTable'] . '.csv.manifest';
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported']['outputTable'] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
        $this->assertEquals(file_get_contents($expectedManifestFile), file_get_contents($outputManifestFile));
    }

    public function testSSHRun(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'remoteHost' => 'firebird',
            'remotePort' => '3050',
            'localPort' => '33335',
        ];

        $app = $this->makeApplication($config);

        $result = $app->run();

        $expectedCsvFile = $this->dataDir . '/firebird/' . $result['imported'][0]['outputTable'] . '.csv';
        $expectedManifestFile = $this->dataDir . '/firebird/' . $result['imported'][0]['outputTable'] . '.csv.manifest';
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0]['outputTable'] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
        $this->assertEquals(file_get_contents($expectedManifestFile), file_get_contents($outputManifestFile));
    }

    public function testTestConnection(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'testConnection';
        $app = $this->makeApplication($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
    }

    public function testTestRowConnection(): void
    {
        $config = $this->getConfigRow(self::DRIVER);
        $config['action'] = 'testConnection';
        $app = $this->makeApplication($config);

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
    }

    public function testSSHConnection(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'testConnection';
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getPrivateKey(),
                'public' => $this->getPublicKey(),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'remoteHost' => 'firebird',
            'remotePort' => '3050',
        ];
        unset($config['parameters']['tables']);

        $app = $this->makeApplication($config);

        $result = $app->run();
        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function testGetTables(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'getTables';

        $app = $this->makeApplication($config);

        $result = $app->run();

        $this->assertArrayHasKey('status', $result);
        $this->assertArrayHasKey('tables', $result);
        $this->assertEquals('success', $result['status']);
        $this->assertCount(11, $result['tables']);

        $expectedTables = [
            0 => [
                'name' => 'COUNTRY',
                'view' => false,
                'columns' => $this->expectedTableColumns('COUNTRY'),
            ],
            1 => [
                'name' => 'JOB',
                'view' => false,
                'columns' => $this->expectedTableColumns('JOB'),
            ],
            2 => [
                'name' => 'DEPARTMENT',
                'view' => false,
                'columns' => $this->expectedTableColumns('DEPARTMENT'),
            ],
            3 => [
                'name' => 'EMPLOYEE',
                'view' => false,
                'columns' => $this->expectedTableColumns('EMPLOYEE'),
            ],
            4 => [
                'name' => 'SALES',
                'view' => false,
                'columns' => $this->expectedTableColumns('SALES'),
            ],
            5 => [
                'name' => 'PHONE_LIST',
                'view' => false,
                'columns' => $this->expectedTableColumns('PHONE_LIST'),
            ],
            6 => [
                'name' => 'PROJECT',
                'view' => false,
                'columns' => $this->expectedTableColumns('PROJECT'),
            ],
            7 => [
                'name' => 'EMPLOYEE_PROJECT',
                'view' => false,
                'columns' => $this->expectedTableColumns('EMPLOYEE_PROJECT'),
            ],
            8 => [
                'name' => 'PROJ_DEPT_BUDGET',
                'view' => false,
                'columns' => $this->expectedTableColumns('PROJ_DEPT_BUDGET'),
            ],
            9 => [
                'name' => 'SALARY_HISTORY',
                'view' => false,
                'columns' => $this->expectedTableColumns('SALARY_HISTORY'),
            ],
            10 => [
                'name' => 'CUSTOMER',
                'view' => false,
                'columns' => $this->expectedTableColumns('CUSTOMER'),
            ],
        ];

        $this->assertEquals($expectedTables, $result['tables']);
    }

    public function testInvalidConfigurationQueryAndTable(): void
    {
        $config = $this->getConfig();
        $config['parameters']['tables'][1]['query'] = 'SELECT 1';

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Both table and query cannot be set together.');
        $this->makeApplication($config);
    }

    public function testInvalidConfigurationQueryNorTable(): void
    {
        $config = $this->getConfig(self::DRIVER);
        unset($config['parameters']['tables'][0]['query']);
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('One of table or query is required');
        $app = $this->makeApplication($config);
    }

    /**
     * @dataProvider simpleTableColumnsDataProvider
     */
    public function testGetSimplifiedPdoQuery(array $params, array $state, string $expected): void
    {
        $config = $this->getConfig();
        $extractor = new Firebird($config['parameters'], $state, new Logger('mssql-extractor-test'));

        $query = $extractor->simpleQuery($params['table'], $params['columns']);
        $this->assertEquals($expected, $query);
    }

    public function simpleTableColumnsDataProvider(): array
    {
        return [
            'simple table select with no column metadata' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => [],
                ],
                [],
                'SELECT * FROM test',
            ],
            'simple table with 2 columns selected' => [
                [
                    'table' => [
                        'tableName' => 'test',
                        'schema' => 'testSchema',
                    ],
                    'columns' => [
                        'col1',
                        'col2',
                    ],
                ],
                [],
                'SELECT col1, col2 FROM test',
            ],
        ];
    }
}
