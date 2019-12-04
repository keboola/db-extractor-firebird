<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

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
}
