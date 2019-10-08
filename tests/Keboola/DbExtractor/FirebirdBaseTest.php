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

    protected function makeApplication(array $config): FirebirdApplication
    {
        return new FirebirdApplication($config, $this->dataDir, $this->logger);
    }

    public function getPrivateKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa');
    }

    public function getPublicKey(): string
    {
        return (string) file_get_contents('/root/.ssh/id_rsa.pub');
    }
}
