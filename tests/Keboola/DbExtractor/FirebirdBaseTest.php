<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\FirebirdApplication;
use Keboola\DbExtractorLogger\Logger;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Yaml\Yaml;

abstract class FirebirdBaseTest extends ExtractorTest
{
    public const DRIVER = 'firebird';

    /** @var Logger */
    private $logger;

    public function setUp(): void
    {
        $this->dataDir = __DIR__ . '/../../data';
        $this->logger = new Logger('firebird-tests');

        // clean the output directory
        @array_map('unlink', (array) glob($this->dataDir . '/out/tables/*.*'));
    }

    protected function getConfig(string $driver = self::DRIVER): array
    {
        $config = json_decode((string) file_get_contents($this->dataDir . '/' . $driver . '/config.json'), true);

        $config = $this->getConfigValues($driver, $config);

        return $config;
    }

    protected function getConfigRow(string $driver): array
    {
        $config = json_decode((string) file_get_contents($this->dataDir . '/' . $driver . '/configRow.json'), true);

        $config = $this->getConfigValues($driver, $config);

        return $config;
    }

    private function getConfigValues(string $driver, array $config): array
    {
        $config['parameters']['data_dir'] = $this->dataDir;
        $config['parameters']['extractor_class'] = 'Firebird';

        if (false === getenv(strtoupper($driver) . '_DB_USER')) {
            throw new \Exception('DB_USER envrionment variable must be set.');
        }

        if (false === getenv(strtoupper($driver) . '_DB_PASSWORD')) {
            throw new \Exception('DB_PASSWORD envrionment variable must be set.');
        }

        $config['parameters']['db']['user'] = getenv(strtoupper($driver) . '_DB_USER');
        $config['parameters']['db']['#password'] = getenv(strtoupper($driver) . '_DB_PASSWORD');

        if (false !== getenv(strtoupper($driver) . '_DB_DBNAME')) {
            $config['parameters']['db']['dbname'] = getenv(strtoupper($driver) . '_DB_DBNAME');
        }

        return $config;
    }

    protected function makeApplication(array $config, array $state = []): FirebirdApplication
    {
        return new FirebirdApplication($config, $this->dataDir, $this->logger, $state);
    }

    public function getPrivateKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa');
    }

    public function getPublicKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa.pub');
    }

    public function expectedTableColumns(string $table): array
    {
        switch (strtoupper($table)) {
            case 'COUNTRY':
                return [
                    0 => [
                        'name' => 'COUNTRY',
                        'type' => 'STRING',
                        'length' => 15,
                        'nullable' => false,
                    ],
                    1 => [
                        'name' => 'CURRENCY',
                        'type' => 'STRING',
                        'length' => 10,
                        'nullable' => false,
                    ],
                ];
            case 'JOB':
                return [
                    0 => [
                        'name'=> 'JOB_CODE',
                        'type'=> 'STRING',
                        'length'=> 5,
                        'nullable'=> false,
                    ],
                    1 => [
                        'name'=> 'JOB_GRADE',
                        'type'=> 'INTEGER',
                        'length'=> 2,
                        'nullable'=> false,
                    ],
                    2 => [
                        'name'=> 'JOB_COUNTRY',
                        'type'=> 'STRING',
                        'length'=> 15,
                        'nullable'=> false,
                    ],
                    3 => [
                        'name'=> 'JOB_TITLE',
                        'type'=> 'STRING',
                        'length'=> 25,
                        'nullable'=> false,
                    ],
                    4 => [
                        'name'=> 'MIN_SALARY',
                        'type'=> 'INTEGER',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                    5 => [
                        'name'=> 'MAX_SALARY',
                        'type'=> 'INTEGER',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                    6 => [
                        'name'=> 'JOB_REQUIREMENT',
                        'type'=> 'STRING',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                    7 => [
                        'name'=> 'LANGUAGE_REQ',
                        'type'=> 'STRING',
                        'length'=> 15,
                        'nullable'=> false,
                    ],
                ];
            case 'DEPARTMENT':
                return [
                    0 => [
                        'name'=> 'DEPT_NO',
                        'type'=> 'STRING',
                        'length'=> 3,
                        'nullable'=> false,
                    ],
                    1 => [
                        'name'=> 'DEPARTMENT',
                        'type'=> 'STRING',
                        'length'=> 25,
                        'nullable'=> false,
                    ],
                    2 => [
                        'name'=> 'HEAD_DEPT',
                        'type'=> 'STRING',
                        'length'=> 3,
                        'nullable'=> false,
                    ],
                    3 => [
                        'name'=> 'MNGR_NO',
                        'type'=> 'INTEGER',
                        'length'=> 2,
                        'nullable'=> false,
                    ],
                    4 => [
                        'name'=> 'BUDGET',
                        'type'=> 'INTEGER',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                    5 => [
                        'name'=> 'LOCATION',
                        'type'=> 'STRING',
                        'length'=> 15,
                        'nullable'=> false,
                    ],
                    6 => [
                        'name'=> 'PHONE_NO',
                        'type'=> 'STRING',
                        'length'=> 20,
                        'nullable'=> false,
                    ],
                ];
            case 'EMPLOYEE':
                return [
                    0 => [
                        'name'=> 'EMP_NO',
                        'type'=> 'INTEGER',
                        'length'=> 2,
                        'nullable'=> false,
                    ],
                    1 => [
                        'name'=> 'FIRST_NAME',
                        'type'=> 'STRING',
                        'length'=> 15,
                        'nullable'=> false,
                    ],
                    2 => [
                        'name'=> 'LAST_NAME',
                        'type'=> 'STRING',
                        'length'=> 20,
                        'nullable'=> false,
                    ],
                    3 => [
                        'name'=> 'PHONE_EXT',
                        'type'=> 'STRING',
                        'length'=> 4,
                        'nullable'=> false,
                    ],
                    4 => [
                        'name'=> 'HIRE_DATE',
                        'type'=> 'TIMESTAMP',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                    5 => [
                        'name'=> 'DEPT_NO',
                        'type'=> 'STRING',
                        'length'=> 3,
                        'nullable'=> false,
                    ],
                    6 => [
                        'name'=> 'JOB_CODE',
                        'type'=> 'STRING',
                        'length'=> 5,
                        'nullable'=> false,
                    ],
                    7 => [
                        'name'=> 'JOB_GRADE',
                        'type'=> 'INTEGER',
                        'length'=> 2,
                        'nullable'=> false,
                    ],
                    8 => [
                        'name'=> 'JOB_COUNTRY',
                        'type'=> 'STRING',
                        'length'=> 15,
                        'nullable'=> false,
                    ],
                    9 => [
                        'name'=> 'SALARY',
                        'type'=> 'INTEGER',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                    10 => [
                        'name'=> 'FULL_NAME',
                        'type'=> 'STRING',
                        'length'=> 37,
                        'nullable'=> false,
                    ],
                ];
            case 'SALES':
                return [
                    0 => [
                        'name'=> 'PO_NUMBER',
                        'type'=> 'STRING',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                    1 => [
                        'name'=> 'CUST_NO',
                        'type'=> 'INTEGER',
                        'length'=> 4,
                        'nullable'=> false,
                    ],
                    2 => [
                        'name'=> 'SALES_REP',
                        'type'=> 'INTEGER',
                        'length'=> 2,
                        'nullable'=> false,
                    ],
                    3 => [
                        'name'=> 'ORDER_STATUS',
                        'type'=> 'STRING',
                        'length'=> 7,
                        'nullable'=> false,
                    ],
                    4 => [
                        'name'=> 'ORDER_DATE',
                        'type'=> 'TIMESTAMP',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                    5 => [
                        'name'=> 'SHIP_DATE',
                        'type'=> 'TIMESTAMP',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                    6 => [
                        'name'=> 'DATE_NEEDED',
                        'type'=> 'TIMESTAMP',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                    7 => [
                        'name'=> 'PAID',
                        'type'=> 'STRING',
                        'length'=> 1,
                        'nullable'=> false,
                    ],
                    8 => [
                        'name'=> 'QTY_ORDERED',
                        'type'=> 'INTEGER',
                        'length'=> 4,
                        'nullable'=> false,
                    ],
                    9 => [
                        'name'=> 'TOTAL_VALUE',
                        'type'=> 'INTEGER',
                        'length'=> 4,
                        'nullable'=> false,
                    ],
                    10 => [
                        'name'=> 'DISCOUNT',
                        'type'=> 'FLOAT',
                        'length'=> 4,
                        'nullable'=> false,
                    ],
                    11 => [
                        'name'=> 'ITEM_TYPE',
                        'type'=> 'STRING',
                        'length'=> 12,
                        'nullable'=> false,
                    ],
                    12 => [
                        'name'=> 'AGED',
                        'type'=> 'INTEGER',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                ];
            case 'PHONE_LIST':
                return [
                    0 => [
                        'name'=> 'EMP_NO',
                        'type'=> 'INTEGER',
                        'length'=> 2,
                        'nullable'=> false,
                    ],
                    1 => [
                        'name'=> 'FIRST_NAME',
                        'type'=> 'STRING',
                        'length'=> 15,
                        'nullable'=> false,
                    ],
                    2 => [
                        'name'=> 'LAST_NAME',
                        'type'=> 'STRING',
                        'length'=> 20,
                        'nullable'=> false,
                    ],
                    3 => [
                        'name'=> 'PHONE_EXT',
                        'type'=> 'STRING',
                        'length'=> 4,
                        'nullable'=> false,
                    ],
                    4 => [
                        'name'=> 'LOCATION',
                        'type'=> 'STRING',
                        'length'=> 15,
                        'nullable'=> false,
                    ],
                    5 => [
                        'name'=> 'PHONE_NO',
                        'type'=> 'STRING',
                        'length'=> 20,
                        'nullable'=> false,
                    ],
                ];
            case 'PROJECT':
                return [
                    0 => [
                        'name'=> 'PROJ_ID',
                        'type'=> 'STRING',
                        'length'=> 5,
                        'nullable'=> false,
                    ],
                    1 => [
                        'name'=> 'PROJ_NAME',
                        'type'=> 'STRING',
                        'length'=> 20,
                        'nullable'=> false,
                    ],
                    2 => [
                        'name'=> 'PROJ_DESC',
                        'type'=> 'STRING',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                    3 => [
                        'name'=> 'TEAM_LEADER',
                        'type'=> 'INTEGER',
                        'length'=> 2,
                        'nullable'=> false,
                    ],
                    4 => [
                        'name'=> 'PRODUCT',
                        'type'=> 'STRING',
                        'length'=> 12,
                        'nullable'=> false,
                    ],
                ];
            case 'EMPLOYEE_PROJECT':
                return [
                    0 => [
                        'name'=> 'EMP_NO',
                        'type'=> 'INTEGER',
                        'length'=> 2,
                        'nullable'=> false,
                    ],
                    1 => [
                        'name'=> 'PROJ_ID',
                        'type'=> 'STRING',
                        'length'=> 5,
                        'nullable'=> false,
                    ],
                ];
            case 'PROJ_DEPT_BUDGET':
                return [
                    0 => [
                        'name'=> 'FISCAL_YEAR',
                        'type'=> 'INTEGER',
                        'length'=> 4,
                        'nullable'=> false,
                    ],
                    1 => [
                        'name'=> 'PROJ_ID',
                        'type'=> 'STRING',
                        'length'=> 5,
                        'nullable'=> false,
                    ],
                    2 => [
                        'name'=> 'DEPT_NO',
                        'type'=> 'STRING',
                        'length'=> 3,
                        'nullable'=> false,
                    ],
                    3 => [
                        'name'=> 'QUART_HEAD_CNT',
                        'type'=> 'INTEGER',
                        'length'=> 4,
                        'nullable'=> false,
                    ],
                    4 => [
                        'name'=> 'PROJECTED_BUDGET',
                        'type'=> 'INTEGER',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                ];
            case 'SALARY_HISTORY':
                return [
                    0 => [
                        'name'=> 'EMP_NO',
                        'type'=> 'INTEGER',
                        'length'=> 2,
                        'nullable'=> false,
                    ],
                    1 => [
                        'name'=> 'CHANGE_DATE',
                        'type'=> 'TIMESTAMP',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                    2 => [
                        'name'=> 'UPDATER_ID',
                        'type'=> 'STRING',
                        'length'=> 20,
                        'nullable'=> false,
                    ],
                    3 => [
                        'name'=> 'OLD_SALARY',
                        'type'=> 'INTEGER',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                    4 => [
                        'name'=> 'PERCENT_CHANGE',
                        'type'=> 'FLOAT',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                    5 => [
                        'name'=> 'NEW_SALARY',
                        'type'=> 'FLOAT',
                        'length'=> 8,
                        'nullable'=> false,
                    ],
                ];
            case 'CUSTOMER':
                return [
                    0 => [
                        'name'=> 'CUST_NO',
                        'type'=> 'INTEGER',
                        'length'=> 4,
                        'nullable'=> false,
                    ],
                    1 => [
                        'name'=> 'CUSTOMER',
                        'type'=> 'STRING',
                        'length'=> 25,
                        'nullable'=> false,
                    ],
                    2 => [
                        'name'=> 'CONTACT_FIRST',
                        'type'=> 'STRING',
                        'length'=> 15,
                        'nullable'=> false,
                    ],
                    3 => [
                        'name'=> 'CONTACT_LAST',
                        'type'=> 'STRING',
                        'length'=> 20,
                        'nullable'=> false,
                    ],
                    4 => [
                        'name'=> 'PHONE_NO',
                        'type'=> 'STRING',
                        'length'=> 20,
                        'nullable'=> false,
                    ],
                    5 => [
                        'name'=> 'ADDRESS_LINE1',
                        'type'=> 'STRING',
                        'length'=> 30,
                        'nullable'=> false,
                    ],
                    6 => [
                        'name'=> 'ADDRESS_LINE2',
                        'type'=> 'STRING',
                        'length'=> 30,
                        'nullable'=> false,
                    ],
                    7 => [
                        'name'=> 'CITY',
                        'type'=> 'STRING',
                        'length'=> 25,
                        'nullable'=> false,
                    ],
                    8 => [
                        'name'=> 'STATE_PROVINCE',
                        'type'=> 'STRING',
                        'length'=> 15,
                        'nullable'=> false,
                    ],
                    9 => [
                        'name'=> 'COUNTRY',
                        'type'=> 'STRING',
                        'length'=> 15,
                        'nullable'=> false,
                    ],
                    10 => [
                        'name'=> 'POSTAL_CODE',
                        'type'=> 'STRING',
                        'length'=> 12,
                        'nullable'=> false,
                    ],
                    11 => [
                        'name'=> 'ON_HOLD',
                        'type'=> 'STRING',
                        'length'=> 1,
                        'nullable'=> false,
                    ],
                ];
            default:
                throw new \Exception('Unknown table name');
        }
    }
}
