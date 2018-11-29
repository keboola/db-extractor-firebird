<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\FirebirdApplication;
use Keboola\DbExtractor\Logger;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Yaml\Yaml;


class FirebirdBaseTest extends ExtractorTest
{
    public const DRIVER = 'firebird';

    /** @var Logger */
    private $logger;

    public function setUp()
    {
        $this->dataDir = __DIR__ . '/../../data';
        $this->logger = new Logger('firebird-tests');

        // clean the output directory
        @array_map('unlink', glob($this->dataDir . "/out/tables/*.*"));
    }

    protected function getConfig(string $driver = self::DRIVER, string $format = self::CONFIG_FORMAT_YAML): array
    {
        if ($format === self::CONFIG_FORMAT_YAML) {
            $config = Yaml::parse(file_get_contents($this->dataDir . '/' . $driver . '/config.yml'));
        } else if ($format === self::CONFIG_FORMAT_JSON) {
            $config = json_decode(file_get_contents($this->dataDir . '/' . $driver . '/config.json'), true);
        } else {
            throw new \Exception("Invalid Test Configuration file");
        }

        $config['parameters']['data_dir'] = $this->dataDir;
        $config['parameters']['extractor_class'] = 'Firebird';

        if (false === getenv(strtoupper($driver) . '_DB_USER')) {
            throw new \Exception("DB_USER envrionment variable must be set.");
        }

        if (false === getenv(strtoupper($driver) . '_DB_PASSWORD')) {
            throw new \Exception("DB_PASSWORD envrionment variable must be set.");
        }

        $config['parameters']['db']['user'] = getenv(strtoupper($driver) . '_DB_USER');
        $config['parameters']['db']['password'] = getenv(strtoupper($driver) . '_DB_PASSWORD');

        if (false !== getenv(strtoupper($driver) . '_DB_DBNAME')) {
            $config['parameters']['db']['dbname'] = getenv(strtoupper($driver) . '_DB_DBNAME');
        }

        return $config;
    }

    protected function makeApplication(array $config): FirebirdApplication
    {
        return new FirebirdApplication($config, $this->logger, [], $this->dataDir);
    }

    public function getFirebirdPrivateKey(): string
    {
        // docker-compose .env file does not support new lines in variables so we have to modify the key https://github.com/moby/moby/issues/12997
        return str_replace('"', '', str_replace('\n', "\n", $this->getEnv('firebird', 'DB_SSH_KEY_PRIVATE')));
    }

    public function configTypeProvider(): array
    {
        return [
            [self::CONFIG_FORMAT_YAML],
            [self::CONFIG_FORMAT_JSON],
        ];
    }
}
