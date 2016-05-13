#!/usr/bin/env bash

composer update -n;

export ROOT_PATH="/code";

./vendor/bin/phpunit;
