#!/bin/bash
{
  echo "Installing docker compose ..."
  wget -O /usr/local/bin/docker-compose "https://github.com/docker/compose/releases/download/v$DOCKER_COMPOSE_VERSION/docker-compose-linux-x86_64"
  ln -sf /usr/local/bin/docker-compose /usr/bin/docker-compose
  chmod +x /usr/local/bin/docker-compose
} > /usr/tmp/startup.logs