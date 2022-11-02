#!/bin/bash

Runtime="${ARUNA_RUNTIME:-docker}"

$Runtime stop dataproxy
$Runtime rm dataproxy
# Stop the minio service
$Runtime stop minio
$Runtime rm minio
# Stop the cockroach service and cleanup
$Runtime stop roach
$Runtime rm roach
$Runtime volume rm roach
# Stop fake keycloak
$Runtime stop fake-keycloak
$Runtime rm fake-keycloak