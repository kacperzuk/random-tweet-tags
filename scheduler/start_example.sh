#!/bin/bash

export AMQP_CONN_STRING=
export PGDATABASE=postgres
export PGUSER=postgres
export PGPASSWORD=mysecretpassword
export PGHOST=localhost

python3 "$@"
