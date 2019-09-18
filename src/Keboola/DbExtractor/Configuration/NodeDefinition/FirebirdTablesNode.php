<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\Configuration\NodeDefinition;

use Keboola\DbExtractorConfig\Configuration\NodeDefinition\TablesNode;

class FirebirdTablesNode extends TablesNode
{
    protected function init(): void
    {
        // @formatter:off
        $this
            ->prototype('array')
            ->children()
                ->integerNode('id')
                    ->isRequired()
                    ->min(0)
                ->end()
                ->scalarNode('name')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('query')
                    ->isRequired()
                ->end()
                ->scalarNode('outputTable')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->booleanNode('incremental')
                    ->defaultValue(false)
                ->end()
                ->booleanNode('enabled')
                    ->defaultValue(true)
                ->end()
                ->arrayNode('primaryKey')
                    ->prototype('scalar')->end()
                ->end()
                ->integerNode('retries')
                    ->min(0)
                ->end()
            ->end()
        ;
        // @formatter:on
    }
}
