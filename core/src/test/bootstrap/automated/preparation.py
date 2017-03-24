import abc
import collections
import itertools
import logging
import os
import posixpath
import shlex
import time

import common
from common import node_types
from java_properties import JavaProperties
from ssh_client import ssh_with_config
from util import await_futures
from common_exceptions import HostOfUnknownTypeException

logger = logging.getLogger(__name__)


def get_file_path_on_file_server(config, path):
    return "{username}@{host}:{path}".format(
        username=config.username,
        host=config.file_server,
        path=path)


def scp_dist_from_file_server(ssh_client, config, dist_name):
    if ssh_client.path_exists(dist_name):
        return
    if not ssh_client.scp(get_file_path_on_file_server(config, dist_name)):
        raise RuntimeError("Unable to upload {} on {} from {}.".format(
            dist_name, ssh_client.host, config.file_server))


def ensure_epel_installed(ssh_client):
    if ssh_client.check_output("yum repolist enabled", "epel/x86_64"):
        return
    ssh_client.sudo_exec("yum install -y epel-release")


def install_prerequisites(ssh_client):
    ensure_epel_installed(ssh_client)
    ssh_client.sudo_exec("yum install -y java-1.8.0-openjdk-devel htop sshpass unzip atop")


class NodePreparator(object):
    def __init__(self, ssh_client, config):
        super().__init__()
        self._ssh_client = ssh_client
        self._config = config

    def prepare(self):
        self._ssh_client.stop_process("java")
        if self._config.public_key_path is not None:
            self._ssh_client.ssh_copy_id(self._config.public_key_path)
        install_prerequisites(self._ssh_client)
        self._ssh_client.sudo_exec("sed -i 's/INTERVAL=.*/INTERVAL={}/' /etc/sysconfig/atop".format(
            common.atop_report_period))
        self._ssh_client.sudo_exec("systemctl restart atop.service")
        common.specific_prepare_hook(self._ssh_client)
        self._prepare_hook()

    @abc.abstractmethod
    def _prepare_hook(self):
        pass


class IgniteNodePreparator(NodePreparator):
    def __init__(self, ssh_client, config, redeploy_activestore):
        super().__init__(ssh_client=ssh_client, config=config)
        self._redeploy_activestore = redeploy_activestore

    def _prepare_hook(self):
        if self._redeploy_activestore:
            # ToDo: might be unsafe.
            self._ssh_client.sudo_exec("rm -rf activestore*")
        if not self._ssh_client.path_exists("activestore-tests"):
            tests_package_name = os.path.basename(self._config.tests_package_path)
            if not self._ssh_client.path_exists(tests_package_name):
                self._obtain_tests_package()
            self._ssh_client.ssh_exec("unzip " + tests_package_name)
            time.sleep(1)

    @abc.abstractmethod
    def _obtain_tests_package(self):
        pass


class StatisticsNodePreparator(IgniteNodePreparator):
    def _prepare_hook(self):
        super()._prepare_hook()
        self._bootstrap_ganglia()
        # ToDo: add support to distinguish if it is a url or local path provided.
        self._ssh_client.wget(self._config.zookeeper_download_url)
        self._ssh_client.wget(self._config.kafka_download_url)

    def _bootstrap_ganglia(self):
        self._ssh_client.sudo_exec("yum install -y rrdtool ganglia ganglia-gmetad ganglia-gmond"
                                   " ganglia-web httpd php apr apr-util")

        self._ssh_client.stop_process("gmond")
        self._ssh_client.sudo_exec("systemctl stop gmetad.service")
        self._ssh_client.sudo_exec("systemctl stop httpd.service")

        # Make ganglia-web accessible from outside.
        httpd_conf_overwrite_command = "echo {} > /etc/httpd/conf.d/ganglia.conf".format(
            shlex.quote(common.httpd_ganglia_conf))
        self._ssh_client.sudo_exec("sh -c {}".format(shlex.quote(httpd_conf_overwrite_command)))

        default_gmond_config = posixpath.join(self._config.remote_home, "gmond-default.conf")
        cluster_gmond_config_path_pattern = posixpath.join(self._config.remote_home, "gmond-{}.conf")
        self._ssh_client.ssh_exec("gmond --default_config > {}".format(default_gmond_config))
        self._start_gmond_receivers(default_gmond_config, cluster_gmond_config_path_pattern)
        # Start sender-receiver gmond for statistics host only to preserve accessibility of custom metrics
        # on Ganglia master when we shutdown gmond sender on cluster nodes.
        self._start_gmond(default_gmond_config, cluster_gmond_config_path_pattern, NodeType.STATISTICS_HOST,
                          common.generic_gmond_config_sed_pattern)
        self._configure_gmetad_collector()

        self._ssh_client.sudo_exec("systemctl start gmetad.service")
        self._ssh_client.sudo_exec("systemctl start httpd.service")

    def _configure_gmetad_collector(self):
        gmetad_config_path = "/etc/ganglia/gmetad.conf"
        gmetad_config_overwrite_command = "rm -f {config} && touch {config}".format(
            config=gmetad_config_path)
        for node_type in node_types:
            gmetad_config_overwrite_command += (" && " + common.gmetad_echo_source_line_pattern.format(
                cluster_name=node_type.cluster_name,
                hostname=self._config.statistics_host,
                config=gmetad_config_path,
                port=node_type.gmond_port))
        for pattern in common.gmetad_config_base_edits:
            gmetad_config_overwrite_command += (" && " + pattern.format(config=gmetad_config_path))
        gmetad_config_overwrite_command = "sh -c {}".format(
            shlex.quote(gmetad_config_overwrite_command))
        self._ssh_client.sudo_exec(gmetad_config_overwrite_command)

    def _start_gmond_receivers(self, default_config_path, custom_config_path_pattern):
        sender_node_types = (node_type for node_type in node_types
                             if node_type is not node_types.STATISTICS_HOST)
        for node_type in sender_node_types:
            self._start_gmond(default_config_path, custom_config_path_pattern, node_type,
                              common.gmond_receiver_config_sed_pattern)

    def _start_gmond(self, default_config_path, custom_config_path_pattern, node_type, config_sed_pattern):
        gmond_receiver_sed = config_sed_pattern.format(
            default_config=default_config_path,
            cluster_name=node_type.cluster_name,
            port=node_type.gmond_port,
            hostname=self._config.statistics_host)
        gmond_config_path = custom_config_path_pattern.format(node_type.cluster_name)
        self._ssh_client.ssh_exec(gmond_receiver_sed + " > " + gmond_config_path)
        # If running with ``ganglia`` user gmond requires to be started as root.
        self._ssh_client.sudo_exec("gmond --conf=" + gmond_config_path)

    def _obtain_tests_package(self):
        logger.info("Uploading activestore package to statistics host.")
        self._ssh_client.upload(self._config.tests_package_path)


def _setup_start_gmond_sender(ssh_client, config):
    ssh_client.sudo_exec("yum install -y ganglia-gmond")
    ssh_client.stop_process("gmond")
    default_gmond_config = posixpath.join(config.remote_home, "gmond-default.conf")
    ssh_client.ssh_exec("gmond --default_config > {}".format(default_gmond_config))
    node_type = config.determine_node_type(ssh_client.host)
    gmond_sender_sed = common.gmond_sender_config_sed_pattern.format(
        default_config=default_gmond_config,
        cluster_name=node_type.cluster_name,
        port=node_type.gmond_port,
        hostname=config.statistics_host)
    gmond_config_path = posixpath.join(config.remote_home, "gmond.conf")
    ssh_client.ssh_exec(gmond_sender_sed + " > " + gmond_config_path)
    ssh_client.sudo_exec("gmond --conf=" + gmond_config_path)
    logger.info("Started gmond sender on %s.", ssh_client.host)


class ZookeeperKafkaNodePreparator(NodePreparator):
    def _prepare_hook(self):
        _setup_start_gmond_sender(self._ssh_client, self._config)
        if not self._ssh_client.path_exists(self._dir_name):
            scp_dist_from_file_server(self._ssh_client, self._config, self._dist_name)
            self._ssh_client.ssh_exec("tar -zxf " + self._dist_name)
        else:
            self._ssh_client.sudo_exec("chmod -R 777 " + self._dir_name)
        self._modify_properties()

    @abc.abstractmethod
    def _modify_properties(self):
        pass

    @property
    @abc.abstractmethod
    def _dist_name(self):
        pass

    @property
    @abc.abstractmethod
    def _dir_name(self):
        pass


class ZookeeperNodePreparator(ZookeeperKafkaNodePreparator):
    def _modify_properties(self):
        properties_path = "{}/conf/zoo.cfg".format(self._dir_name)
        self._ssh_client.ssh_exec("rm -f " + properties_path)
        self._ssh_client.ssh_exec("cp {}/conf/zoo_sample.cfg {}".format(self._dir_name, properties_path))
        self._ssh_client.modify_file(properties_path, JavaProperties.read, self._modify_zk_server_properties,
                                     JavaProperties.dump)

    def _modify_zk_server_properties(self, properties):
        properties.update(common.zookeeper_properties)
        for i, host in enumerate(self._config.zookeeper_hosts, 1):
            key = "server.{}".format(i)
            value = "{}:2888:3888".format(host)
            properties[key] = value

    @property
    def _dist_name(self):
        return self._config.zookeeper_dist_name

    @property
    def _dir_name(self):
        return self._config.zookeeper_dir_name


class KafkaNodePreparator(ZookeeperKafkaNodePreparator):
    def _modify_properties(self):
        properties_path = "{}/config/server.properties".format(self._dir_name)
        self._ssh_client.modify_file(properties_path, JavaProperties.read, self._modify_kafka_server_properties,
                                     JavaProperties.dump)

    def _modify_kafka_server_properties(self, properties):
        zk_servers = ",".join(host + ":2181" for host in self._config.zookeeper_hosts)
        broker_id = self._config.kafka_hosts.index(self._ssh_client.host)
        properties.update(common.kafka_properties)
        properties["broker.id"] = broker_id
        properties["zookeeper.connect"] = zk_servers

    @property
    def _dist_name(self):
        return self._config.kafka_dist_name

    @property
    def _dir_name(self):
        return self._config.kafka_dir_name


class ClientServerNodePreparator(IgniteNodePreparator):
    def _prepare_hook(self):
        super()._prepare_hook()
        _setup_start_gmond_sender(self._ssh_client, self._config)

    def _obtain_tests_package(self):
        tests_package_name = os.path.basename(self._config.tests_package_path)
        scp_dist_from_file_server(self._ssh_client, self._config, tests_package_name)


class _PrepareHelper(collections.abc.Callable):
    def __init__(self, preparator_cls):
        super().__init__()
        self._preparator_cls = preparator_cls

    def __call__(self, host, config, *args, **kwargs):
        with ssh_with_config(host, config) as ssh_client:
            preparator = self._preparator_cls(ssh_client, config, *args, **kwargs)
            preparator.prepare()
        try:
            node_type = config.determine_node_type(host)
            logger.info("Bootstrapped %s on %s.", node_type.cluster_name, host)
        except HostOfUnknownTypeException:
            logger.info("Bootstrapped %s.", host)

prepare_statistics_node = _PrepareHelper(StatisticsNodePreparator)

prepare_client_server_node = _PrepareHelper(ClientServerNodePreparator)

prepare_zookeeper_node = _PrepareHelper(ZookeeperNodePreparator)

prepare_kafka_node = _PrepareHelper(KafkaNodePreparator)


def prepare_nodes(hosts, executor, prepare_node, *args, **kwargs):
    await_futures(executor.submit(prepare_node, *args, host=host, **kwargs) for host in hosts)


def prepare_cluster(config, redeploy_activestore, executor):
    logger.info("Deploying activestore package to cluster nodes.")
    prepare_statistics_node(config.statistics_host, config, redeploy_activestore)

    ignite_hosts = itertools.chain(config.server_hosts, config.client_hosts)
    prepare_nodes(ignite_hosts, executor, prepare_client_server_node, config=config,
                  redeploy_activestore=redeploy_activestore)

    logger.info("Deploying kafka/zookeeper distributions.")
    prepare_nodes(config.zookeeper_hosts, executor, prepare_zookeeper_node, config=config)
    prepare_nodes(config.kafka_hosts, executor, prepare_kafka_node, config=config)
