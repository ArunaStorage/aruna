#!/bin/bash

Runtime="${ARUNA_RUNTIME:-docker}"

$Runtime run -d -p 9000:9000 -p 9001:9001 --name minio -e MINIO_DOMAIN=localhost:9000 quay.io/minio/minio server /data --console-address ":9001" 

# Create database
psql "postgres://yugabyte@localhost:5433" -c 'CREATE DATABASE proxy' 

# Import schema (script has to be called from project root)
psql "postgres://yugabyte@localhost:5433/proxy" < $(pwd)/src/database/schema.sql