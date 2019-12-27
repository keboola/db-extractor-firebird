<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;


class FirebirdIncrementalTest extends FirebirdBaseTest
{

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
}
