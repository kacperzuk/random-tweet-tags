#!/bin/bash

RABBITMQCTL="docker exec rabbitmq rabbitmqctl"

$RABBITMQCTL list_queues
