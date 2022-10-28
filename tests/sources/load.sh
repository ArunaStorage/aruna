#!/bin/bash

Runtime="${ARUNA_RUNTIME:-docker}"

# Load testdata
$Runtime exec roach cockroach sql --insecure -f /cockroach/data/up.sql