#!/usr/bin/env python3

import logging
import signal
import time
import os
from servicemanager import servicemanager as sm

sm.init()

def stop_instance(service, instance):
    pid = sm.get_pid(service, instance)

    os.kill(-pid, signal.SIGINT)
    for i in range(10):
        time.sleep(0.1)
        try:
            os.kill(pid, 0)
        except OSError:
            clean_pidfile(service, instance)
            return

    os.kill(-pid, signal.SIGINT)
    for i in range(10):
        time.sleep(0.1)
        try:
            os.kill(pid, 0)
        except OSError:
            clean_pidfile(service, instance)
            return

    # failed to kill process group :(

def clean_pidfile(service, instance):
    os.unlink(sm.get_pid_path(service, instance))

for service in sm.get_services():
    for instance in sm.get_instances(service):
        status = sm.get_instance_status(service, instance)
        if status == "crashed":
            logging.info("%s (%s) crashed, clearing pidfile..." % (service, instance))
            clean_pidfile(service, instance)
        elif status == "stopped":
            logging.info("Skipping %s (%s), already stopped." % (service, instance))
        else:
            logging.info("Stopping %s (%s)." % (service, instance))
            stop_instance(service, instance)

sm.pretty_print_statuses()
