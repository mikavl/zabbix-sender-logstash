#!/usr/bin/env python3

import datetime
import json
import logging
import os
import subprocess
import sys
import tempfile
import threading

import kubernetes.client
import kubernetes.config
import kubernetes.stream
import pyzabbix

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DISABLED_TAG = "disabled"
HOST_FORMAT = "{}-{}"
LOGGER = "zsl"

class Config(object):
    pass

def create_tag(k, v):
    return {"tag": k, "value": v}

def get_tag(host, k):
    return next((tag["value"] for tag in host["tags"] if tag["tag"] == k), None)

def read_stats(v1, pod):
    stats = kubernetes.stream.stream(v1.connect_get_namespaced_pod_exec,
        pod.metadata.name,
        pod.metadata.namespace,
        command=["/usr/bin/curl", "-fsSL", "http://127.0.0.1:9600/_node/stats"],
        stdout=True,
        stderr=False,
        stdin=False,
        tty=False,
        _preload_content=False)
    stats.run_forever(timeout=5)
    return json.loads(stats.read_stdout())

def write_stats(f, instance):

    stats = []

    def g(p, d):
        for k, v in d.items():
            if isinstance(v, dict):
                g(p + [k], v)
            elif isinstance(v, list):
                j = 0
                for i in v:
                    g(p + [k, str(j)], i)
                    j += 1
            else:
                if isinstance(v, str):
                    value = '"{}"'.format(v)
                elif isinstance(v, int):
                    value = int(v)
                else:
                    value = v

                stats.append("{} {}.{} {}\n".format(instance["host"], ".".join(p), k, value))

    g(["logstash"], instance["stats"])

    f.writelines(stats)
    f.flush()
    os.fsync(f)

def timer_loop(config):
    logger = logging.getLogger(LOGGER)

    logger.info("Scheduling timer, interval %d seconds", config.interval_seconds)
    try:
        timer = threading.Timer(config.interval_seconds, timer_loop, (config,))
        timer.start()
    except Exception as e:
        logger.critical("Failed to schedule timer: %s", e)
        sys.exit(1)

    logger.info("Listing pods in all Kubernetes namespaces")
    try:
        kubernetes.config.load_kube_config(config_file=config.kubernetes_config_file)
        v1 = kubernetes.client.CoreV1Api()
        pods = v1.list_pod_for_all_namespaces(label_selector=config.label_selector, watch=False)
    except Exception as e:
        logger.error("Failed to list Kubernetes pods: %s", e)
        return

    logger.info("Got %d pods, reading Logstash statistics", len(pods.items))
    instances = {}
    for pod in pods.items:
        host = HOST_FORMAT.format(pod.metadata.namespace, pod.metadata.name)

        try:
            stats = read_stats(v1, pod)
        except Exception as e:
            logger.error("Failed to read statistics from pod '%s' in namespace '%s': %s" , pod.metadata.name, pod.metadata.namespace, e)
            return

        instances[host] = {
            "name": pod.metadata.name,
            "namespace": pod.metadata.namespace,
            "host": host,
            "stats": stats,
        }

    logger.info("Successfully read Logstash statistics from pods: %s", ", ".join(["{} ({})".format(v["name"], v["namespace"]) for k, v in instances.items()]))
    logger.info("Reading host information from Zabbix server")

    try:
        zabbix_api = pyzabbix.ZabbixAPI(config.zabbix_server_url)
        zabbix_api.login(api_token=config.zabbix_api_token)
        hosts = zabbix_api.host.get(selectTags="extend", tags=[config.tag])
    except Exception as e:
        logger.error("Failed to read host information from Zabbix server: %s")

    logger.info("Successfully read %d hosts from Zabbix server", len(hosts))
    for k, instance in instances.items():

        host_exists = False

        for host in hosts:
            if host["host"] == instance["host"]:
                host_exists = True
                break

        if not host_exists:
            logger.info("Host '%s' does not exist in Zabbix server, creating", host["host"])
            try:
                zabbix_api.host.create(
                    host=instance["host"],
                    groups=[{"groupid": config.host_group_id}],
                    tags=[config.tag],
                    templates=[{"templateid": config.template_id}]
                )
            except Exception as e:
                logger.error("Failed to create host '%s' in Zabbix server: %s", host["host"], e)
                return

    for host in hosts:
        try:
            instances[host["host"]]
        except KeyError:

            disabled = get_tag(host, DISABLED_TAG)
            if disabled:
                disabled_datetime = datetime.datetime.strptime(disabled, DATETIME_FORMAT)
                if disabled_datetime < datetime.datetime.now() - datetime.timedelta(days=config.host_delete_interval_days):
                    logger.info("Host '%s' has been disabled for %s which is longer than the configured period of %d days, deleting host from Zabbix server", host["host"], datetime.datetime.now() - disabled_datetime, config.host_delete_interval_days)

                    try:
                        zabbix_api.host.delete(hostid=host["hostid"])
                    except Exception as e:
                        logger.error("Failed to delete host '%s' from Zabbix server: %s", host["host"], e)
                        return

            else:
                logger.info("Instance '%s' not found in Kubernetes but host present in Zabbix server, disabling host", host["host"])
                disabled_tag = create_tag("disabled", datetime.datetime.now().strftime(DATETIME_FORMAT))

                try:
                    zabbix_api.host.update(hostid=host["hostid"], status=1, tags=host["tags"] + [disabled_tag])
                except Exception as e:
                    logger.error("Failed to disable host '%s' in Zabbix server: %s", host["host"], e)
                    return

    with tempfile.NamedTemporaryFile(mode="w") as f:
        logger.info("Writing statistics to temporary file '%s'", f.name)

        for k, instance in instances.items():
            try:
                write_stats(f, instance)
            except Exception as e:
                logger.error("Failed to write statistics to temporary file '%s': %s", f.name, e)
                return

        logger.info("Sending statistics to Zabbix trapper at '%s:%d'", config.zabbix_trapper_address, config.zabbix_trapper_port)
        try:
            process = subprocess.Popen(["/usr/bin/zabbix_sender",
                "--zabbix-server", config.zabbix_trapper_address,
                "--port", str(config.zabbix_trapper_port),
                "--input-file", f.name], shell=False,
                stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            process.wait()

            if process.returncode == 1:
                raise Exception(process.stderr.read())

            stdout = process.stdout.read()
        except Exception as e:
            logger.error("Failed to write statistics to Zabbix trapper at '%s:%d': '%s'", config.zabbix_trapper_address, config.zabbix_trapper_port, e)
            return

        logger.info("Successfully sent statistics to Zabbix trapper: %s", stdout.decode("utf-8").strip().replace("\n", " "))


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
        datefmt=DATETIME_FORMAT
    )
    logger = logging.getLogger(LOGGER)
    logger.setLevel(logging.INFO)

    config = Config()
    config.interval_seconds = os.getenv("ZSL_INTERVAL_SECONDS", 60)
    config.host_delete_interval_days = os.getenv("ZSL_HOST_DELETE_INTERVAL_DAYS", 7)
    config.kubernetes_config_file = os.getenv("ZSL_KUBERNETES_CONFIG_FILE")
    config.label_selector = os.getenv("ZSL_LABEL_SELECTOR", "app.kubernetes.io/name=logstash")
    config.zabbix_api_token = os.getenv("ZSL_ZABBIX_API_TOKEN")
    config.zabbix_server_url = os.getenv("ZSL_ZABBIX_SERVER_URL")
    config.zabbix_trapper_address = os.getenv("ZSL_ZABBIX_TRAPPER_ADDRESS")
    config.zabbix_trapper_port = os.getenv("ZSL_ZABBIX_TRAPPER_PORT", 10051)

    host_group_name = os.getenv("ZSL_HOST_GROUP_NAME", "Logstash")
    template_name = os.getenv("ZSL_TEMPLATE_NAME", "Logstash")

    if config.zabbix_api_token == None or config.zabbix_server_url == None or config.zabbix_trapper_address == None:
        logger.critical("ZSL_ZABBIX_API_TOKEN, ZSL_ZABBIX_SERVER_URL and ZSL_ZABBIX_TRAPPER_ADDRESS need to be set")
        sys.exit(1)

    try:
        kv = config.label_selector.split("=")
        config.tag = create_tag(kv[0], kv[1])
    except IndexError:
        logger.critical("Failed to create tag from label selector, ensure it is in the 'key=value' format: '%s'", config.label_selector)
        sys.exit(1)

    try:
        zabbix_api = pyzabbix.ZabbixAPI(config.zabbix_server_url)
        zabbix_api.login(api_token=config.zabbix_api_token)
        config.host_group_id = zabbix_api.hostgroup.get(filter={"name": host_group_name})[0]["groupid"]
        config.template_id = zabbix_api.template.get(filter={"name": template_name})[0]["templateid"]
    except Exception as e:
        logger.critical("Failed to read host group and template information from Zabbix server: '%s'", e)
        sys.exit(1)

    timer_loop(config)
