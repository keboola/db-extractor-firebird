<?php


namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Test\ExtractorTest;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class RunTest extends ExtractorTest
{
    public const ROOT_PATH = __DIR__ . '/../../..';

    public function testTestConnectionAction(): void
    {
        $config = $this->getConfig('firebird');
        $config['action'] = 'testConnection';
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
        $this->assertJson($process->getOutput());
    }

    public function testSshTestConnection(): void
    {
        $config = $this->getConfig('firebird');
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
            'localPort' => '33338'
        ];
        $config['action'] = 'testConnection';
        @unlink($this->dataDir . '/config.yml');
        file_put_contents($this->dataDir . '/config.yml', Yaml::dump($config));

        $process = new Process('php ' . ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals("", $process->getErrorOutput());
        $this->assertJson($process->getOutput());
    }

    public function getFirebirdPrivateKey(): string
    {
        // docker-compose .env file does not support new lines in variables so we have to modify the key https://github.com/moby/moby/issues/12997
        return str_replace('"', '', str_replace('\n', "\n", $this->getEnv('firebird', 'DB_SSH_KEY_PRIVATE')));
    }
}
