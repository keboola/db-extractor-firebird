<?php

use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Configuration\FirebirdConfigDefinition;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractor\Logger;
use Monolog\Handler\NullHandler;
use Symfony\Component\Yaml\Yaml;

define('APP_NAME', 'ex-db-firebird');
define('ROOT_PATH', __DIR__);

require_once(dirname(__FILE__) . "/vendor/keboola/db-extractor-common/bootstrap.php");

$logger = new Logger(APP_NAME);

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

    $config['parameters']['data_dir'] = $arguments['data'];
    $config['parameters']['extractor_class'] = 'Firebird';

    $app = new Application($config);
    $app->setConfigDefinition(new FirebirdConfigDefinition());
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
