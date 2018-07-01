import configparser
from collections import defaultdict
import logging
import os

__parent_path = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
__pids_path = os.path.join(__parent_path, "run", "pids")
__logs_path = os.path.join(__parent_path, "run", "logs")
__bins_path = os.path.join(__parent_path, "bin")

__config = None

def init(config_path=None):
    global __config

    if not os.path.exists(__pids_path):
        os.makedirs(__pids_path)

    if not os.path.exists(__logs_path):
        os.makedirs(__logs_path)

    if not config_path:
        config_path = os.path.join(__parent_path, "services.ini")

    if not __config:
        cp = configparser.ConfigParser()
        cp.read(config_path)

        __config = {}
        for name, proxy in cp.items():
            if name == "DEFAULT":
                __config["DEFAULT"] = dict(proxy)
            else:
                service, instance = name.split("/")
                if not service in __config:
                    __config[service] = {}
                __config[service][instance] = dict(proxy)

def get_instances(service):
    if service not in __config:
        return ["0"]
    return sorted(__config[service].keys())

def get_env(service, instance):
    env = {}
    for k, v in __config["DEFAULT"].items():
        env[k.upper()] = v
    try:
        for k,v in __config[service][instance].items():
            env[k.upper()] = v
    except KeyError:
        pass
    return env

def get_services():
    return sorted([ f for f in os.listdir(__bins_path) if os.access(get_executable_path(f), os.X_OK)])

def get_executable_path(service):
    return os.path.join(__bins_path, service)

def get_pid_path(service, instance):
    return os.path.join(__pids_path, "%s.%s.pid" % (service, instance))

def get_log_path(service, instance):
    return os.path.join(__logs_path, "%s.%s.log" % (service, instance))

def get_pid(service, instance):
    try:
        with open(get_pid_path(service, instance), 'r') as f:
            return int(f.read().strip())
    except IOError:
        return None

def check_pid_running(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True

def get_instance_status(service, instance):
    pid = get_pid(service, instance)
    if not pid:
        status = "stopped"
    elif check_pid_running(pid):
        status = "running; PID: %d" % pid
    else:
        status = "crashed"
    return status

def pretty_print_statuses():
    services = get_services()
    max_length = max(len(s) for s in services)
    row_format = ("{:>%d}\t{:<15}\t{}" % max_length)
    print(row_format.format("Service", "Instance", "Status"))
    for service in get_services():
        for instance in get_instances(service):
            print(row_format.format(service, instance, get_instance_status(service, instance)))
