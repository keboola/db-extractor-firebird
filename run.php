<?php

use Keboola\DbExtractor\FirebirdApplication;
use Keboola\DbExtractor\Configuration\FirebirdConfigDefinition;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Monolog\Handler\NullHandler;
use Symfony\Component\Yaml\Yaml;

require_once(__DIR__ . "/vendor/autoload.php");

$logger = new Logger('ex-db-firebird');

try {
    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new UserException('Data folder not set.');
    }

    if (file_exists($arguments["data"] . "/config.yml")) {
        $config = Yaml::parse(file_get_contents($arguments["data"] . "/config.yml"));
    } else if (file_exists($arguments["data"] . "/config.json")) {
        $config = json_decode(file_get_contents($arguments["data"] . "/config.yml"), true);
    } else {
        throw new UserException("Could not find a valid configuration file.");
    }

    $app = new FirebirdApplication($config, $logger, [], $arguments['data']);

    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers([new NullHandler(Logger::INFO)]);
    }
    echo json_encode($app->run());

} catch(UserException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());
    exit(1);
} catch(ApplicationException $e) {
    $logger->log('error', $e->getMessage(), (array) $e->getData());
    exit($e->getCode() > 1 ? $e->getCode(): 2);
} catch(\Throwable $e) {
    $logger->log('error', $e->getMessage(), [
        'errFile' => $e->getFile(),
        'errLine' => $e->getLine(),
        'trace' => $e->getTrace()
    ]);
    exit(2);
}

exit(0);
