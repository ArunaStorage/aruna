#!/bin/bash
#apt-get update
#apt-get install --yes --no-install-recommends postgresql-client 

Runtime="${ARUNA_RUNTIME:-docker}"

Network="bridge"
if [ "$Runtime" == "podman" ] ;
then
    Network="podman"
fi

# Start Nats.io container (No cluster port)
$Runtime run -d --name nats-js-server --rm -p 4222:4222 -p 8222:8222 nats:latest --http_port 8222 --js

# Start Meilisearch container
$Runtime run -d --rm -p 7700:7700 -e MEILI_MASTER_KEY='MASTER_KEY' getmeili/meilisearch:latest

# Start yugabyte container
$Runtime run -d --name yugabyte -p5433:5433 yugabytedb/yugabyte:latest bin/yugabyted start --daemon=false

until [ "${Runtime} inspect -f {{.State.Running}} yugabyte"=="true" ]; do
    echo "Waiting for container startup ..."
    sleep 1;
done;

# Give the container some time to be available
while ! $Runtime logs yugabyte | grep -q "Data placement constraint successfully verified";
do
    sleep 1
    echo "db initializing..."
done

# Create database
psql "postgres://yugabyte@localhost:5433" -c 'CREATE DATABASE test' 

# Import schema (script has to be called from project root)
psql "postgres://yugabyte@localhost:5433/test" < $(pwd)/src/database/schema.sql
