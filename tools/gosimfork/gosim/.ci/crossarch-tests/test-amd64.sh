#!/bin/bash
cd "${0%/*}"
set -e

docker compose -f ./docker-compose.yml run --build --rm test-linux-amd64
