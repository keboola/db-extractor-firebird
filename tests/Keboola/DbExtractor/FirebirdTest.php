<?php

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\FirebirdConfigDefinition;
use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Yaml\Yaml;


class FirebirdTest extends ExtractorTest
{
    /** @var Application */
    protected $app;

    public function setUp()
    {
        if (!defined('APP_NAME')) {
            define('APP_NAME', 'ex-db-firebird');
        }
        $this->app = new Application($this->getConfig());
        $this->app->setConfigDefinition(new FirebirdConfigDefinition());

        // clean the output directory
        @array_map('unlink', glob($this->dataDir . "/out/tables/*.*"));
    }

    protected function getConfig($driver = 'firebird')
    {
        $config = Yaml::parse(file_get_contents($this->dataDir . '/' . $driver . '/config.yml'));
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

    public function testRun(): void
    {
        $result = $this->app->run();
        $expectedCsvFile = ROOT_PATH . '/tests/data/firebird/' . $result['imported'][0] . '.csv';
        $expectedManifestFile = ROOT_PATH . '/tests/data/firebird/' . $result['imported'][0] . '.csv.manifest';
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
        $this->assertEquals(file_get_contents($expectedManifestFile), file_get_contents($outputManifestFile));
    }

    public function testSSHRun(): void
    {
        $config = $this->getConfig();
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getFirebirdPrivateKey(),
                'public' => $this->getEnv('firebird', 'DB_SSH_KEY_PUBLIC'),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'remoteHost' => 'firebird',
            'remotePort' => $this->getEnv('firebird', 'DB_PORT'),
            'localPort' => '33335'
        ];

        $app = new Application($config);
        $app->setConfigDefinition(new FirebirdConfigDefinition());

        $result = $app->run();

        $expectedCsvFile = ROOT_PATH . '/tests/data/firebird/' . $result['imported'][0] . '.csv';
        $expectedManifestFile = ROOT_PATH . '/tests/data/firebird/' . $result['imported'][0] . '.csv.manifest';
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('success', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
        $this->assertEquals(file_get_contents($expectedManifestFile), file_get_contents($outputManifestFile));
    }

    public function testTestConnection(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        $app = new Application($config);
        $app->setConfigDefinition(new FirebirdConfigDefinition());

        $result = $app->run();
        $this->assertEquals('success', $result['status']);
    }

    public function testSSHConnection(): void
    {
        $config = $this->getConfig();
        $config['action'] = 'testConnection';
        $config['parameters']['db']['ssh'] = [
            'enabled' => true,
            'keys' => [
                '#private' => $this->getFirebirdPrivateKey(),
                'public' => $this->getEnv('firebird', 'DB_SSH_KEY_PUBLIC'),
            ],
            'user' => 'root',
            'sshHost' => 'sshproxy',
            'remoteHost' => 'firebird',
            'remotePort' => $this->getEnv('firebird', 'DB_PORT'),
        ];
        unset($config['parameters']['tables']);

        $app = new Application($config);
        $app->setConfigDefinition(new FirebirdConfigDefinition());

        $result = $app->run();
        $this->assertArrayHasKey('status', $result);
        $this->assertEquals('success', $result['status']);
    }

    public function getFirebirdPrivateKey(): string
    {
        // docker-compose .env file does not support new lines in variables so we have to modify the key https://github.com/moby/moby/issues/12997
        return str_replace('"', '', str_replace('\n', "\n", $this->getEnv('firebird', 'DB_SSH_KEY_PRIVATE')));
    }
}
