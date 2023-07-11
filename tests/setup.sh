#!/bin/bash
#apt-get update
#apt-get install --yes --no-install-recommends postgresql-client 
docker run -d --name yugabyte -p5433:5433 yugabytedb/yugabyte:latest bin/yugabyted start --daemon=false

until [ "docker inspect -f {{.State.Running}} yugabyte"=="true" ]; do
    sleep 1;
done;
# Give the container some time to be available
sleep 10;


psql "postgres://yugabyte@localhost:5433" -c 'CREATE DATABASE test' 