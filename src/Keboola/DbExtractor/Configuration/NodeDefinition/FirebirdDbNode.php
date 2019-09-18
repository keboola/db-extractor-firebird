<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\DbNode;

class FirebirdDbNode extends DbNode
{
    protected function init(): void
    {
        // @formatter:off
        $this
            ->children()
                ->scalarNode('dbname')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('host')->end()
                ->scalarNode('port')->end()
                ->scalarNode('user')
                    ->isRequired()
                ->end()
                ->scalarNode('#password')
                    ->isRequired()
                ->end()
                ->append($this->sshNode)
            ->end()
        ;
        // @formatter:on
    }
}
