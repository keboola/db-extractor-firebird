<?php

use Keboola\DbExtractor\FirebirdApplication;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;
use Keboola\DbExtractorLogger\Logger;
use Monolog\Handler\NullHandler;

require_once(__DIR__ . "/vendor/autoload.php");

$logger = new Logger('ex-db-firebird');

try {
    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new UserException('Data folder not set.');
    }

    if (file_exists($arguments["data"] . "/config.json")) {
        $config = json_decode(file_get_contents($arguments["data"] . "/config.json"), true);
    } else {
        throw new UserException("Could not find a valid configuration file.");
    }

    $app = new FirebirdApplication($config, $arguments['data'], $logger);

    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers([new NullHandler(Logger::INFO)]);
    }
    echo json_encode($app->run());

} catch(UserException|ConfigUserException $e) {
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
