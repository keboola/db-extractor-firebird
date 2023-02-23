<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Tests;

use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Filesystem\Filesystem;
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

    public function testTestConnectionWrongDbNameAction(): void
    {
        $config = $this->getConfig(self::DRIVER);
        $config['action'] = 'testConnection';
        $config['parameters']['db']['dbname'] = 'firebird/3050:d:/usr/local/firebird/examples/empbuild/employee.fdb';
        $this->replaceConfig($config);

        $process = new Process(['php', self::ROOT_PATH . '/run.php', '--data=' . $this->dataDir]);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(
            "Connection failed: 'Error connecting to DB: SQLSTATE[HY000] [335544375] unavailable database'",
            $process->getErrorOutput()
        );
        $this->assertEquals(1, $process->getExitCode());
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

    public function testRunActionSshTunnel(): void
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
            'localPort' => '33339',
        ];

        $this->replaceConfig($config);

        $process = new Process(['php', self::ROOT_PATH . '/run.php', '--data=' . $this->dataDir]);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertStringContainsString('Creating SSH tunnel to \'sshproxy\'', $process->getOutput());
        $this->assertStringContainsString(
            'Connecting to "firebird:dbname=127.0.0.1/33339:/usr/local/firebird/examples/empbuild/employee.fdb"',
            $process->getOutput()
        );
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

    public function testRunIncrementalFetching(): void
    {
        $config = $this->getIncrementalConfig();
        $this->createAutoIncrementAndTimestampTable($config);
        $this->insertAutoIncrementAndTimestampTableData($config);

        @unlink($this->dataDir . '/config.json');

        $inputStateFile = $this->dataDir . '/in/state.json';

        $fs = new Filesystem();
        if (!$fs->exists($inputStateFile)) {
            $fs->mkdir($this->dataDir . '/in');
            $fs->touch($inputStateFile);
        }
        $outputStateFile = $this->dataDir . '/out/state.json';
        // unset the state file
        @unlink($outputStateFile);
        @unlink($inputStateFile);

        file_put_contents($this->dataDir . '/config.json', json_encode($config));

        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertFileExists($outputStateFile);
        $this->assertEquals(['lastFetchedRow' => '2'], json_decode((string) file_get_contents($outputStateFile), true));

        // add a couple rows
        $this->insertAutoIncrementAndTimestampTableData($config);

        // copy state to input state file
        file_put_contents($inputStateFile, file_get_contents($outputStateFile));

        // run the config again
        $process = Process::fromShellCommandline('php ' . self::ROOT_PATH . '/run.php --data=' . $this->dataDir);
        $process->setTimeout(300);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
        $this->assertEquals(['lastFetchedRow' => '4'], json_decode((string) file_get_contents($outputStateFile), true));
    }
}
