<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Process\Process;

class RunTest extends FirebirdBaseTest
{
    public const ROOT_PATH = __DIR__ . '/../../..';

    private function replaceConfig(array $config): void
    {
        @unlink($this->dataDir . '/config.json');
        file_put_contents($this->dataDir . '/config.json', json_encode($config));
    }

    public function testTestConnectionAction(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'testConnection';
        $this->replaceConfig($config);

        $process = new Process(['php', self::ROOT_PATH . '/run.php', '--data=' . $this->dataDir]);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
        $this->assertJson($process->getOutput());
    }

    public function testRun(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $this->replaceConfig($config);

        $process = new Process(['php', self::ROOT_PATH . '/run.php', '--data=' . $this->dataDir]);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
    }

    public function testSshTestConnection(): void
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
            'localPort' => '33338',
        ];
        $config['action'] = 'testConnection';
        $this->replaceConfig($config);

        $process = new Process(['php', self::ROOT_PATH . '/run.php', '--data=' . $this->dataDir]);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals('', $process->getErrorOutput());
        $this->assertJson($process->getOutput());
    }
}
