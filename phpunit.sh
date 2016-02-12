#!/usr/bin/env bash

env;

composer install -n;

./vendor/bin/phpunit --verbose --debug;