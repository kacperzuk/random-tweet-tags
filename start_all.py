#!/usr/bin/env python3

import logging
import os
import subprocess
import time
from servicemanager import servicemanager as sm

sm.init()

def start_instance(service, instance):
    env = dict(os.environ, **sm.get_env(service, instance))
    pid = subprocess.Popen(
            [sm.get_executable_path(service)],
            stdout=open(sm.get_log_path(service, instance), 'w'),
            stderr=subprocess.STDOUT,
            env=env,
            preexec_fn=os.setpgrp).pid

    with open(sm.get_pid_path(service, instance), 'w') as f:
        f.write(str(pid))

for service in sm.get_services():
    for instance in sm.get_instances(service):
        if sm.get_instance_status(service, instance) in ("stopped", "crashed"):
            logging.info("Starting %s (%s)..." % (service, instance))
            start_instance(service, instance)
        else:
            logging.info("Skipping %s (%s), already running." % (service, instance))

time.sleep(1)

sm.pretty_print_statuses()
