<?php

use Keboola\DbExtractor\Application;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Symfony\Component\Yaml\Yaml;

require_once(dirname(__FILE__) . "/vendor/keboola/db-extractor-common/bootstrap.php");

define('APP_NAME', 'ex-db-firebird');

try {

    $arguments = getopt("d::", ["data::"]);
    if (!isset($arguments["data"])) {
        throw new UserException('Data folder not set.');
    }

    $config = Yaml::parse(file_get_contents($arguments["data"] . "/config.yml"));
    $config['dataDir'] = $arguments['data'];

    $app = new Application($config);
    $app->run();

} catch(UserException $e) {

    $app['logger']->log('error', $e->getMessage(), (array) $e->getData());
    exit(1);

} catch(ApplicationException $e) {

    $app['logger']->log('error', $e->getMessage(), (array) $e->getData());
    exit($e->getCode() > 1 ? $e->getCode(): 2);

} catch(\Exception $e) {

    $app['logger']->log('error', $e->getMessage(), [
        'errFile' => $e->getFile(),
        'errLine' => $e->getLine(),
        'trace' => $e->getTrace()
    ]);
    exit(2);

}

$app['logger']->log('info', "Writer finished successfully.");
exit(0);