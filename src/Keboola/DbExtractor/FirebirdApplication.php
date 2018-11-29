<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\FirebirdConfigDefinition;

class FirebirdApplication extends \Keboola\DbExtractor\Application
{
    public function __construct(array $config, Logger $logger, array $state = [], string $dataDir)
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'Firebird';

        parent::__construct($config, $logger, $state);

        $this->setConfigDefinition(new FirebirdConfigDefinition());
    }
}
