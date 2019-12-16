<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\FirebirdApplication;
use Keboola\DbExtractorLogger\Logger;
use Keboola\DbExtractor\Test\ExtractorTest;
use Keboola\DbExtractor\Exception\UserException;

abstract class FirebirdBaseTest extends ExtractorTest
{
    public const DRIVER = 'firebird';

    private Logger $logger;


    private \PDO $connection;

    public function setUp(): void
    {
        $this->dataDir = __DIR__ . '/../../data';
        $this->logger = new Logger('firebird-tests');

        // clean the output directory
        @array_map('unlink', (array) glob($this->dataDir . '/out/tables/*.*'));

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
                        'sanitizedName' => 'COUNTRY',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    1 => [
                        'name' => 'CURRENCY',
                        'type' => 'STRING',
                        'length' => 10,
                        'nullable' => false,
                        'sanitizedName' => 'CURRENCY',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                ];
            case 'JOB':
                return [
                    0 => [
                        'name' => 'JOB_CODE',
                        'type' => 'STRING',
                        'length' => 5,
                        'nullable' => false,
                        'sanitizedName' => 'JOB_CODE',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    1 => [
                        'name' => 'JOB_GRADE',
                        'type' => 'INTEGER',
                        'length' => '2',
                        'nullable' => false,
                        'sanitizedName' => 'JOB_GRADE',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    2 => [
                        'name' => 'JOB_COUNTRY',
                        'type' => 'STRING',
                        'length' => '15',
                        'nullable' => false,
                        'sanitizedName' => 'JOB_COUNTRY',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    3 => [
                        'name' => 'JOB_TITLE',
                        'type' => 'STRING',
                        'length' => '25',
                        'nullable' => false,
                        'sanitizedName' => 'JOB_TITLE',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    4 => [
                        'name' => 'MIN_SALARY',
                        'type' => 'INTEGER',
                        'length' => '8',
                        'nullable' => false,
                        'sanitizedName' => 'MIN_SALARY',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    5 => [
                        'name' => 'MAX_SALARY',
                        'type' => 'INTEGER',
                        'length' => '8',
                        'nullable' => false,
                        'sanitizedName' => 'MAX_SALARY',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    6 => [
                        'name' => 'JOB_REQUIREMENT',
                        'type' => 'STRING',
                        'length' => '8',
                        'nullable' => false,
                        'sanitizedName' => 'JOB_REQUIREMENT',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    7 => [
                        'name' => 'LANGUAGE_REQ',
                        'type' => 'STRING',
                        'length' => '15',
                        'nullable' => false,
                        'sanitizedName' => 'LANGUAGE_REQ',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                ];
            case 'DEPARTMENT':
                return [
                    0 => [
                        'name' => 'DEPT_NO',
                        'type' => 'STRING',
                        'length' => 3,
                        'nullable' => false,
                        'sanitizedName' => 'DEPT_NO',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    1 => [
                        'name' => 'DEPARTMENT',
                        'type' => 'STRING',
                        'length' => 25,
                        'nullable' => false,
                        'sanitizedName' => 'DEPARTMENT',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    2 => [
                        'name' => 'HEAD_DEPT',
                        'type' => 'STRING',
                        'length' => 3,
                        'nullable' => false,
                        'sanitizedName' => 'HEAD_DEPT',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    3 => [
                        'name' => 'MNGR_NO',
                        'type' => 'INTEGER',
                        'length' => 2,
                        'nullable' => false,
                        'sanitizedName' => 'MNGR_NO',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    4 => [
                        'name' => 'BUDGET',
                        'type' => 'INTEGER',
                        'length' => 8,
                        'nullable' => false,
                        'sanitizedName' => 'BUDGET',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    5 => [
                        'name' => 'LOCATION',
                        'type' => 'STRING',
                        'length' => 15,
                        'nullable' => false,
                        'sanitizedName' => 'LOCATION',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    6 => [
                        'name' => 'PHONE_NO',
                        'type' => 'STRING',
                        'length' => 20,
                        'nullable' => false,
                        'sanitizedName' => 'PHONE_NO',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                ];
            case 'EMPLOYEE':
                return [
                    0 => [
                        'name' => 'EMP_NO',
                        'type' => 'INTEGER',
                        'length' => 2,
                        'nullable' => false,
                        'sanitizedName' => 'EMP_NO',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    1 => [
                        'name' => 'FIRST_NAME',
                        'type' => 'STRING',
                        'length' => 15,
                        'nullable' => false,
                        'sanitizedName' => 'FIRST_NAME',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    2 => [
                        'name' => 'LAST_NAME',
                        'type' => 'STRING',
                        'length' => 20,
                        'nullable' => false,
                        'sanitizedName' => 'LAST_NAME',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    3 => [
                        'name' => 'PHONE_EXT',
                        'type' => 'STRING',
                        'length' => 4,
                        'nullable' => false,
                        'sanitizedName' => 'PHONE_EXT',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    4 => [
                        'name' => 'HIRE_DATE',
                        'type' => 'TIMESTAMP',
                        'length' => 8,
                        'nullable' => false,
                        'sanitizedName' => 'HIRE_DATE',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    5 => [
                        'name' => 'DEPT_NO',
                        'type' => 'STRING',
                        'length' => 3,
                        'nullable' => false,
                        'sanitizedName' => 'DEPT_NO',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    6 => [
                        'name' => 'JOB_CODE',
                        'type' => 'STRING',
                        'length' => 5,
                        'nullable' => false,
                        'sanitizedName' => 'JOB_CODE',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    7 => [
                        'name' => 'JOB_GRADE',
                        'type' => 'INTEGER',
                        'length' => 2,
                        'nullable' => false,
                        'sanitizedName' => 'JOB_GRADE',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    8 => [
                        'name' => 'JOB_COUNTRY',
                        'type' => 'STRING',
                        'length' => 15,
                        'nullable' => false,
                        'sanitizedName' => 'JOB_COUNTRY',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    9 => [
                        'name' => 'SALARY',
                        'type' => 'INTEGER',
                        'length' => 8,
                        'nullable' => false,
                        'sanitizedName' => 'SALARY',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    10 => [
                        'name' => 'FULL_NAME',
                        'type' => 'STRING',
                        'length' => 37,
                        'nullable' => false,
                        'sanitizedName' => 'FULL_NAME',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                ];
            case 'SALES':
                return [
                    0 => [
                        'name' => 'PO_NUMBER',
                        'type' => 'STRING',
                        'length' => 8,
                        'nullable' => false,
                        'sanitizedName' => 'PO_NUMBER',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    1 => [
                        'name' => 'CUST_NO',
                        'type' => 'INTEGER',
                        'length' => 4,
                        'nullable' => false,
                        'sanitizedName' => 'CUST_NO',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    2 => [
                        'name' => 'SALES_REP',
                        'type' => 'INTEGER',
                        'length' => 2,
                        'nullable' => false,
                        'sanitizedName' => 'SALES_REP',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    3 => [
                        'name' => 'ORDER_STATUS',
                        'type' => 'STRING',
                        'length' => 7,
                        'nullable' => false,
                        'sanitizedName' => 'ORDER_STATUS',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    4 => [
                        'name' => 'ORDER_DATE',
                        'type' => 'TIMESTAMP',
                        'length' => 8,
                        'nullable' => false,
                        'sanitizedName' => 'ORDER_DATE',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    5 => [
                        'name' => 'SHIP_DATE',
                        'type' => 'TIMESTAMP',
                        'length' => 8,
                        'nullable' => false,
                        'sanitizedName' => 'SHIP_DATE',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    6 => [
                        'name' => 'DATE_NEEDED',
                        'type' => 'TIMESTAMP',
                        'length' => 8,
                        'nullable' => false,
                        'sanitizedName' => 'DATE_NEEDED',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    7 => [
                        'name' => 'PAID',
                        'type' => 'STRING',
                        'length' => 1,
                        'nullable' => false,
                        'sanitizedName' => 'PAID',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    8 => [
                        'name' => 'QTY_ORDERED',
                        'type' => 'INTEGER',
                        'length' => 4,
                        'nullable' => false,
                        'sanitizedName' => 'QTY_ORDERED',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    9 => [
                        'name' => 'TOTAL_VALUE',
                        'type' => 'INTEGER',
                        'length' => 4,
                        'nullable' => false,
                        'sanitizedName' => 'TOTAL_VALUE',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    10 => [
                        'name' => 'DISCOUNT',
                        'type' => 'FLOAT',
                        'length' => 4,
                        'nullable' => false,
                        'sanitizedName' => 'DISCOUNT',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    11 => [
                        'name' => 'ITEM_TYPE',
                        'type' => 'STRING',
                        'length' => 12,
                        'nullable' => false,
                        'sanitizedName' => 'ITEM_TYPE',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    12 => [
                        'name' => 'AGED',
                        'type' => 'INTEGER',
                        'length' => 8,
                        'nullable' => false,
                        'sanitizedName' => 'AGED',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                ];
            case 'PHONE_LIST':
                return [
                    0 => [
                        'name' => 'EMP_NO',
                        'type' => 'INTEGER',
                        'length' => 2,
                        'nullable' => false,
                        'sanitizedName' => 'EMP_NO',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    1 => [
                        'name' => 'FIRST_NAME',
                        'type' => 'STRING',
                        'length' => 15,
                        'nullable' => false,
                        'sanitizedName' => 'FIRST_NAME',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    2 => [
                        'name' => 'LAST_NAME',
                        'type' => 'STRING',
                        'length' => 20,
                        'nullable' => false,
                        'sanitizedName' => 'LAST_NAME',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    3 => [
                        'name' => 'PHONE_EXT',
                        'type' => 'STRING',
                        'length' => 4,
                        'nullable' => false,
                        'sanitizedName' => 'PHONE_EXT',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    4 => [
                        'name' => 'LOCATION',
                        'type' => 'STRING',
                        'length' => 15,
                        'nullable' => false,
                        'sanitizedName' => 'LOCATION',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    5 => [
                        'name' => 'PHONE_NO',
                        'type' => 'STRING',
                        'length' => 20,
                        'nullable' => false,
                        'sanitizedName' => 'PHONE_NO',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                ];
            case 'PROJECT':
                return [
                    0 => [
                        'name' => 'PROJ_ID',
                        'type' => 'STRING',
                        'length' => 5,
                        'nullable' => false,
                        'sanitizedName' => 'PROJ_ID',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    1 => [
                        'name' => 'PROJ_NAME',
                        'type' => 'STRING',
                        'length' => 20,
                        'nullable' => false,
                        'sanitizedName' => 'PROJ_NAME',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    2 => [
                        'name' => 'PROJ_DESC',
                        'type' => 'STRING',
                        'length' => 8,
                        'nullable' => false,
                        'sanitizedName' => 'PROJ_DESC',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    3 => [
                        'name' => 'TEAM_LEADER',
                        'type' => 'INTEGER',
                        'length' => 2,
                        'nullable' => false,
                        'sanitizedName' => 'TEAM_LEADER',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    4 => [
                        'name' => 'PRODUCT',
                        'type' => 'STRING',
                        'length' => 12,
                        'nullable' => false,
                        'sanitizedName' => 'PRODUCT',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                ];
            case 'EMPLOYEE_PROJECT':
                return [
                    0 => [
                        'name' => 'EMP_NO',
                        'type' => 'INTEGER',
                        'length' => 2,
                        'nullable' => false,
                        'sanitizedName' => 'EMP_NO',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    1 => [
                        'name' => 'PROJ_ID',
                        'type' => 'STRING',
                        'length' => 5,
                        'nullable' => false,
                        'sanitizedName' => 'PROJ_ID',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                ];
            case 'PROJ_DEPT_BUDGET':
                return [
                    0 => [
                        'name' => 'FISCAL_YEAR',
                        'type' => 'INTEGER',
                        'length' => 4,
                        'nullable' => false,
                        'sanitizedName' => 'FISCAL_YEAR',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    1 => [
                        'name' => 'PROJ_ID',
                        'type' => 'STRING',
                        'length' => 5,
                        'nullable' => false,
                        'sanitizedName' => 'PROJ_ID',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    2 => [
                        'name' => 'DEPT_NO',
                        'type' => 'STRING',
                        'length' => 3,
                        'nullable' => false,
                        'sanitizedName' => 'DEPT_NO',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    3 => [
                        'name' => 'QUART_HEAD_CNT',
                        'type' => 'INTEGER',
                        'length' => 4,
                        'nullable' => false,
                        'sanitizedName' => 'QUART_HEAD_CNT',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    4 => [
                        'name' => 'PROJECTED_BUDGET',
                        'type' => 'INTEGER',
                        'length' => 8,
                        'nullable' => false,
                        'sanitizedName' => 'PROJECTED_BUDGET',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                ];
            case 'SALARY_HISTORY':
                return [
                    0 => [
                        'name' => 'EMP_NO',
                        'type' => 'INTEGER',
                        'length' => 2,
                        'nullable' => false,
                        'sanitizedName' => 'EMP_NO',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    1 => [
                        'name' => 'CHANGE_DATE',
                        'type' => 'TIMESTAMP',
                        'length' => 8,
                        'nullable' => false,
                        'sanitizedName' => 'CHANGE_DATE',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    2 => [
                        'name' => 'UPDATER_ID',
                        'type' => 'STRING',
                        'length' => 20,
                        'nullable' => false,
                        'sanitizedName' => 'UPDATER_ID',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    3 => [
                        'name' => 'OLD_SALARY',
                        'type' => 'INTEGER',
                        'length' => 8,
                        'nullable' => false,
                        'sanitizedName' => 'OLD_SALARY',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    4 => [
                        'name' => 'PERCENT_CHANGE',
                        'type' => 'FLOAT',
                        'length' => 8,
                        'nullable' => false,
                        'sanitizedName' => 'PERCENT_CHANGE',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    5 => [
                        'name' => 'NEW_SALARY',
                        'type' => 'FLOAT',
                        'length' => 8,
                        'nullable' => false,
                        'sanitizedName' => 'NEW_SALARY',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                ];
            case 'CUSTOMER':
                return [
                    0 => [
                        'name' => 'CUST_NO',
                        'type' => 'INTEGER',
                        'length' => 4,
                        'nullable' => false,
                        'sanitizedName' => 'CUST_NO',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    1 => [
                        'name' => 'CUSTOMER',
                        'type' => 'STRING',
                        'length' => 25,
                        'nullable' => false,
                        'sanitizedName' => 'CUSTOMER',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    2 => [
                        'name' => 'CONTACT_FIRST',
                        'type' => 'STRING',
                        'length' => 15,
                        'nullable' => false,
                        'sanitizedName' => 'CONTACT_FIRST',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    3 => [
                        'name' => 'CONTACT_LAST',
                        'type' => 'STRING',
                        'length' => 20,
                        'nullable' => false,
                        'sanitizedName' => 'CONTACT_LAST',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    4 => [
                        'name' => 'PHONE_NO',
                        'type' => 'STRING',
                        'length' => 20,
                        'nullable' => false,
                        'sanitizedName' => 'PHONE_NO',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    5 => [
                        'name' => 'ADDRESS_LINE1',
                        'type' => 'STRING',
                        'length' => 30,
                        'nullable' => false,
                        'sanitizedName' => 'ADDRESS_LINE1',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    6 => [
                        'name' => 'ADDRESS_LINE2',
                        'type' => 'STRING',
                        'length' => 30,
                        'nullable' => false,
                        'sanitizedName' => 'ADDRESS_LINE2',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    7 => [
                        'name' => 'CITY',
                        'type' => 'STRING',
                        'length' => 25,
                        'nullable' => false,
                        'sanitizedName' => 'CITY',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    8 => [
                        'name' => 'STATE_PROVINCE',
                        'type' => 'STRING',
                        'length' => 15,
                        'nullable' => false,
                        'sanitizedName' => 'STATE_PROVINCE',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    9 => [
                        'name' => 'COUNTRY',
                        'type' => 'STRING',
                        'length' => 15,
                        'nullable' => false,
                        'sanitizedName' => 'COUNTRY',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    10 => [
                        'name' => 'POSTAL_CODE',
                        'type' => 'STRING',
                        'length' => 12,
                        'nullable' => false,
                        'sanitizedName' => 'POSTAL_CODE',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    11 => [
                        'name' => 'ON_HOLD',
                        'type' => 'STRING',
                        'length' => 1,
                        'nullable' => false,
                        'sanitizedName' => 'ON_HOLD',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                ];
            case 'AUTO_INCREMENT_TIMESTAMP':
                return [
                    0 => [
                        'name' => 'ID',
                        'type' => 'INTEGER',
                        'length' => '4',
                        'nullable' => false,
                        'sanitizedName' => 'ID',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    1 => [
                        'name' => 'NAME',
                        'type' => 'STRING',
                        'length' => '255',
                        'nullable' => false,
                        'sanitizedName' => 'NAME',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                    2 => [
                        'name' => 'SOMEDECIMAL',
                        'type' => 'FLOAT',
                        'length' => '4',
                        'nullable' => false,
                        'sanitizedName' => 'SOMEDECIMAL',
                        'primaryKey' => false,
                        'uniqueKey' => false,

                    ],
                    3 => [
                        'name' => 'SOMETIMESTAMP',
                        'type' => 'TIMESTAMP',
                        'length' => '8',
                        'nullable' => false,
                        'sanitizedName' => 'SOMETIMESTAMP',
                        'primaryKey' => false,
                        'uniqueKey' => false,

                    ],
                    4 => [
                        'name' => 'SOMEDATE',
                        'type' => 'DATE',
                        'length' => '4',
                        'nullable' => false,
                        'sanitizedName' => 'SOMEDATE',
                        'primaryKey' => false,
                        'uniqueKey' => false,
                    ],
                ];
            default:
                throw new \Exception('Unknown table name');
        }
    }

    protected function getIncrementalConfig(): array
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

    protected function createAutoIncrementAndTimestampTable(array $config): void
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

    protected function insertAutoIncrementAndTimestampTableData(array $config, int $numberRows = 2): void
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
