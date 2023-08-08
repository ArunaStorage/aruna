#!/bin/bash
#apt-get update
#apt-get install --yes --no-install-recommends postgresql-client 

Runtime="${ARUNA_RUNTIME:-docker}"

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
sleep 5;

# Create database
psql "postgres://yugabyte@localhost:5433" -c 'CREATE DATABASE test' 

# Import schema (script has to be called from project root)
psql "postgres://yugabyte@localhost:5433/test" < $(pwd)/src/database/schema.sql
# Add relation types for testing
psql "postgres://yugabyte@localhost:5433/test" -c "INSERT INTO relation_types (relation_name) VALUES ('BELONGS_TO'), ('VERSION'), ('METADATA'), ('ORIGIN'), ('POLICY') "
