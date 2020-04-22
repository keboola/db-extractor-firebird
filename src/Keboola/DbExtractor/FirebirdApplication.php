<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\NodeDefinition\FirebirdDbNode;
use Keboola\DbExtractor\Configuration\NodeDefinition\FirebirdTablesNode;
use Keboola\DbExtractor\Exception\UserException;
use Keboola\DbExtractorConfig\Config;
use Keboola\DbExtractorConfig\Configuration\ActionConfigRowDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigDefinition;
use Keboola\DbExtractorConfig\Configuration\ConfigRowDefinition;
use Keboola\DbExtractorLogger\Logger;
use Keboola\DbExtractorConfig\Exception\UserException as ConfigUserException;

class FirebirdApplication extends Application
{
    private const DEFAULT_FIREBIRD_PORT = 3050;

    public function __construct(array $config, string $dataDir, Logger $logger, array $state = [])
    {
        if (isset($config['parameters']['db']['ssh']['enabled']) && $config['parameters']['db']['ssh']['enabled']) {
            preg_match('/([^:]+?)\/?([0-9]+)?:(.*)/', $config['parameters']['db']['dbname'], $connectionParts);
            if (count($connectionParts) < 4) {
                throw new UserException('Invalid configuration for ssh tunnel');
            }
            $config['parameters']['db']['host'] = $connectionParts[1];
            $config['parameters']['db']['port'] =
                !empty($connectionParts[2]) ?
                    $connectionParts[2] :
                    self::DEFAULT_FIREBIRD_PORT
            ;
        }
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'Firebird';

        parent::__construct($config, $logger, $state);
    }

    public function buildConfig(array $config): void
    {
        $dbNode = new FirebirdDbNode();
        try {
            if ($this->isRowConfiguration($config)) {
                if ($this['action'] === 'run') {
                    $this->config = new Config($config, new ConfigRowDefinition($dbNode));
                } else {
                    $this->config = new Config($config, new ActionConfigRowDefinition($dbNode));
                }
            } else {
                $this->config = new Config(
                    $config,
                    new ConfigDefinition($dbNode)
                );
            }
        } catch (ConfigUserException $e) {
            throw new UserException($e->getMessage(), $e->getCode(), $e);
        }
    }
}
