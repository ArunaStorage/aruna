#!/bin/bash

# The env var ARUNA_RUNTIME can be used to change the container runtimes
# Default is docker, can be changed to podman if necessary

Runtime="${ARUNA_RUNTIME:-docker}"


Network="bridge"
if [ "$Runtime" == "podman" ] ;
then
    Network="podman"
fi

# Start minio server
$Runtime run -d -p 9000:9000 -p 9001:9001 --net=$Network --name minio\
  quay.io/minio/minio server /data --console-address ":9001"

# Start the DataProxy

$Runtime run -d -p 8080:8080 --net=$Network --name dataproxy stanni/aruna_demo_proxy:v0.0.1

# Start cockroach if not exists
$Runtime run -d \
--name=roach \
--hostname=roach \
-p 26257:26257 -p 9999:9999  \
-v "roach:/cockroach/cockroach-data"  \
-v "$(pwd)/tests/sources/:/cockroach/data/" \
cockroachdb/cockroach:v22.1.5 start-single-node \
--insecure \
