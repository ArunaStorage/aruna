#!/bin/bash
apt-get update
apt-get install --yes --no-install-recommends postgresql-client 
docker run -d --name yugabyte -p5433:5433 yugabytedb/yugabyte:latest bin/yugabyted start --daemon=false
psql "postgres://yugabyte@localhost:5433" -c 'CREATE DATABASE test' 