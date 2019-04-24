<?php

declare(strict_types=1);

namespace Keboola\DbExtractor;

use Keboola\DbExtractor\Configuration\FirebirdConfigDefinition;

class FirebirdApplication extends Application
{
    public function __construct(array $config, string $dataDir, Logger $logger, array $state = [])
    {
        $config['parameters']['data_dir'] = $dataDir;
        $config['parameters']['extractor_class'] = 'Firebird';

        parent::__construct($config, $logger, $state);

        $this->setConfigDefinition(new FirebirdConfigDefinition());
    }
}
