import itertools

import common
from ssh_client import ssh_with_config
from util import await_futures


class Cleaner(object):
    def __init__(self, ssh_client, config):
        super().__init__()
        self._ssh_client = ssh_client
        self._config = config

    def cleanup(self):
        self._ssh_client.sudo_exec("find /var/log/atop/ -mtime +{} -delete".format(
            common.logs_retention_days))
        self._ssh_client.ssh_exec("find {}/logs/ -mtime +{} -delete".format(
            self._config.kafka_dir_name, common.logs_retention_days))
        wildcard_test_name = common.test_name_pattern.format("*", "*")
        self._ssh_client.ssh_exec(
            "find {} -maxdepth 1 -type d -name '{}' -mtime +{}"
            " -exec rm -rf {{}} +".format(self._config.remote_home, wildcard_test_name,
                                          common.logs_retention_days))
        common.specific_cleanup_hook(self._ssh_client)


def cleanup_host(host, config):
    with ssh_with_config(host, config) as ssh_client:
        cleaner = Cleaner(ssh_client, config)
        cleaner.cleanup()


def cleanup_hosts(hosts, config, executor):
    await_futures(executor.submit(cleanup_host, host, config) for host in hosts)


def cleanup_cluster(config, executor):
    hosts = itertools.chain(config.server_hosts, config.client_hosts, config.kafka_hosts,
                            config.zookeeper_hosts, [config.statistics_host])
    cleanup_hosts(hosts, config, executor)
