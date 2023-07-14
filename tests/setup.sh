#!/bin/bash
#apt-get update
#apt-get install --yes --no-install-recommends postgresql-client 

Runtime="${ARUNA_RUNTIME:-docker}"

Network="bridge"
if [ "$Runtime" == "podman" ] ;
then
    Network="podman"
fi

# Start yugabyte container
$Runtime run -d --name yugabyte -p5433:5433 yugabytedb/yugabyte:latest bin/yugabyted start --daemon=false

until [ "${Runtime} inspect -f {{.State.Running}} yugabyte"=="true" ]; do
    echo "Waiting for container startup ..."
    sleep 1;
done;

# Give the container some time to be available
sleep 10;

# Create database
psql "postgres://yugabyte@localhost:5433" -c 'CREATE DATABASE test' 

