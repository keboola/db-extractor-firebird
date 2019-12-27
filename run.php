<?php

use Keboola\DbExtractor\FirebirdApplication;
use Keboola\DbExtractor\Exception\ApplicationException;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;
use Keboola\DbExtractorLogger\Logger;
use Monolog\Handler\NullHandler;
use Symfony\Component\Serializer\Encoder\JsonEncode;
use Symfony\Component\Serializer\Encoder\JsonEncoder;
use Symfony\Component\Serializer\Encoder\JsonDecode;

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

    // get the state
    $inputState = [];
    $inputStateFile = $arguments["data"] . '/in/state.json';
    if (file_exists($inputStateFile)) {
        $jsonDecode = new JsonDecode(true);
        $inputState = $jsonDecode->decode(
            (string) file_get_contents($inputStateFile),
            JsonEncoder::FORMAT
        );
    }

    $app = new FirebirdApplication($config, $arguments['data'], $logger, $inputState);

    $runAction = true;
    if ($app['action'] !== 'run') {
        $app['logger']->setHandlers([new NullHandler(Logger::INFO)]);
        $runAction = false;
    }

    $result = $app->run();

    if (!$runAction) {
        echo json_encode($result);
    } else {
        if (!empty($result['state'])) {
            // write state
            $outputStateFile = $arguments["data"] . '/out/state.json';
            $jsonEncode = new JsonEncode();
            file_put_contents($outputStateFile, $jsonEncode->encode($result['state'], JsonEncoder::FORMAT));
        }
    }
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
