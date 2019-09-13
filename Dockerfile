FROM db-ex-firebird-sshproxy AS sshproxy
FROM php:7.3-cli-stretch

ARG DEBIAN_FRONTEND=noninteractive
ARG COMPOSER_FLAGS="--prefer-dist --no-interaction"
ENV COMPOSER_ALLOW_SUPERUSER=1
ENV COMPOSER_PROCESS_TIMEOUT 3600

RUN apt-get update && apt-get install -y \
        git \
        unzip \
        ssh \
        firebird-dev \
   --no-install-recommends && rm -r /var/lib/apt/lists/*

RUN docker-php-ext-install pdo_firebird && \
    docker-php-ext-enable pdo_firebird

RUN curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin/ --filename=composer

ADD . /code
WORKDIR /code
RUN echo "memory_limit = -1" >> /etc/php.ini
RUN composer install --no-interaction

COPY --from=sshproxy /root/.ssh /root/.ssh

CMD php ./run.php --data=/data
