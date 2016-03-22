<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 22/03/16
 * Time: 12:47
 */
namespace Keboola\DbExtractor\Configuration;

use Symfony\Component\Config\Definition\Builder\TreeBuilder;

class FirebirdConfigDefinition extends ConfigDefinition
{
    /** @inherit */
    public function getConfigTreeBuilder()
    {
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('config');

        $rootNode
            ->children()
            ->scalarNode('data_dir')
            ->isRequired()
            ->cannotBeEmpty()
            ->end()
            ->scalarNode('extractor_class')
            ->isRequired()
            ->cannotBeEmpty()
            ->end()
            ->arrayNode('parameters')
            ->children()
                ->arrayNode('db')
                    ->children()
                        ->scalarNode('dbname')
                            ->isRequired()
                            ->cannotBeEmpty()
                        ->end()
                        ->scalarNode('user')
                            ->isRequired()
                        ->end()
                        ->scalarNode('password')->end()
                        ->scalarNode('#password')->end()
                        ->append($this->addSshNode())
                    ->end()
                ->end()
            ->arrayNode('tables')
            ->isRequired()
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
            ->cannotBeEmpty()
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
            ->scalarNode('primaryKey')
            ->defaultValue(null)
            ->end()
            ->integerNode('retries')
            ->min(1)
            ->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->end()
            ->arrayNode('image_parameters')
            ->end()
            ->end()
        ;

        return $treeBuilder;
    }

}
