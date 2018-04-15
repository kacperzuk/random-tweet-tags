#!/bin/bash

for q in $(sudo rabbitmqctl list_queues | awk '{print $1}' | tail -n+2); do
	sudo rabbitmqctl purge_queue $q &
done

wait

for q in $(sudo rabbitmqctl list_queues | awk '{print $1}' | tail -n+2); do
	sudo rabbitmqctl purge_queue $q &
done

wait
