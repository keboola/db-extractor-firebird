<?php

/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 10/12/15
 * Time: 14:25
 */

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
        define('APP_NAME', 'ex-db-firebird');
        $this->app = new Application($this->getConfig());
        $this->app->setConfigDefinition(new FirebirdConfigDefinition());
    }

    protected function getConfig($driver = 'firebird')
    {
        $config = Yaml::parse(file_get_contents($this->dataDir . '/' .$driver . '/config.yml'));
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

    public function testRun()
    {
        $result = $this->app->run();
        $expectedCsvFile = ROOT_PATH . '/tests/data/firebird/' . $result['imported'][0] . '.csv';
        $expectedManifestFile = ROOT_PATH . '/tests/data/firebird/' . $result['imported'][0] . '.csv.manifest';
        $outputCsvFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv';
        $outputManifestFile = $this->dataDir . '/out/tables/' . $result['imported'][0] . '.csv.manifest';

        $this->assertEquals('ok', $result['status']);
        $this->assertFileExists($outputCsvFile);
        $this->assertFileExists($outputManifestFile);
        $this->assertEquals(file_get_contents($expectedCsvFile), file_get_contents($outputCsvFile));
        $this->assertEquals(file_get_contents($expectedManifestFile), file_get_contents($outputManifestFile));
    }

}
