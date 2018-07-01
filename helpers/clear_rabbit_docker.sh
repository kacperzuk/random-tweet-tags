#!/bin/bash

RABBITMQCTL="docker exec rabbitmq rabbitmqctl"

for q in $($RABBITMQCTL list_queues | awk '{print $1}' | tail -n+2); do
	$RABBITMQCTL purge_queue $q &
done

wait

for q in $($RABBITMQCTL list_queues | awk '{print $1}' | tail -n+2); do
	$RABBITMQCTL purge_queue $q &
done

wait
