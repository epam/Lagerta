import configparser
import logging
import os
import posixpath
import zipfile

import paramiko
from paramiko.ssh_exception import PasswordRequiredException

import common
from common import node_types
from common_exceptions import HostOfUnknownTypeException
from java_properties import JavaProperties
from util import lazy_property

logger = logging.getLogger(__name__)


def check_if_password_is_required(key_path):
    try:
        paramiko.RSAKey.from_private_key_file(key_path, None)
        return False
    except PasswordRequiredException:
        return True


def _strip_tar_extensions(filename):
    if filename.endswith(".tar.gz"):
        return filename[:-7]
    if filename.endswith(".tgz"):
        return filename[:-4]
    return filename


class Configuration(object):
    def __init__(self, statistics_host, server_hosts, client_hosts, port, tests_package_path,
                 username, password, servers_per_test, public_key_path, private_key_path,
                 password_required, zookeeper_download_url, zookeeper_hosts, kafka_download_url,
                 kafka_hosts, full_execution_time):
        super().__init__()
        self.statistics_host = statistics_host
        self.server_hosts = server_hosts
        self.client_hosts = client_hosts
        self.port = port
        self.tests_package_path = tests_package_path
        self.username = username
        self.password = password
        self.servers_per_test = servers_per_test
        self.public_key_path = public_key_path
        self.private_key_path = private_key_path
        self.password_required = password_required
        self.zookeeper_download_url = zookeeper_download_url
        self.zookeeper_hosts = zookeeper_hosts
        self.kafka_download_url = kafka_download_url
        self.kafka_hosts = kafka_hosts
        self.full_execution_time = full_execution_time

    @property
    def file_server(self):
        return self.statistics_host

    @lazy_property
    def remote_home(self):
        return posixpath.join("/home", self.username)

    @lazy_property
    def zookeeper_dist_name(self):
        return self.zookeeper_download_url.rsplit("/", 1)[1]

    @lazy_property
    def zookeeper_dir_name(self):
        return posixpath.join(self.remote_home, _strip_tar_extensions(self.zookeeper_dist_name))

    @lazy_property
    def kafka_dist_name(self):
        return self.kafka_download_url.rsplit("/", 1)[1]

    @lazy_property
    def kafka_dir_name(self):
        return posixpath.join(self.remote_home, _strip_tar_extensions(self.kafka_dist_name))

    def determine_node_type(self, host):
        if host in self.server_hosts:
            return node_types.IGNITE_SERVER
        if host in self.client_hosts:
            return node_types.IGNITE_CLIENT
        if host in self.kafka_hosts:
            return node_types.KAFKA_BROKER
        if host in self.zookeeper_hosts:
            return node_types.ZOOKEEPER_SERVER
        if host == self.statistics_host:
            return node_types.STATISTICS_HOST
        raise HostOfUnknownTypeException("host {} not present in passed config".format(host))

    @classmethod
    def read(cls, config_filepath):
        parser = configparser.ConfigParser(
            allow_no_value=True,
            interpolation=configparser.ExtendedInterpolation())
        parser.read(config_filepath)
        config = parser["load.test"]
        password = config.get("Password")
        tests_package_path = config["TestsPackagePath"]

        properties = _read_settings_from_java_properties(tests_package_path)
        full_execution_time = (int(properties["load.tests.warmup.period"]) +
                               int(properties["load.tests.execution.time"]))
        full_execution_time /= 1000  # Duration in load tests properties is written in milliseconds.
        statistics_host = properties[common.ganglia_host_property_name]

        public_key_path = config.get("PublicKeyPath")
        if public_key_path is not None and not os.path.exists(public_key_path):
            public_key_path = None

        private_key_path = config.get("PrivateKeyPath")
        password_required = check_if_password_is_required(private_key_path)

        if private_key_path is None and password is None:
            logger.warning("No password or private key provided, ssh logins are likely to fail.")
        return cls(
            username=config["User"],
            password=password,
            server_hosts=_get_config_list(config, "ServerHosts"),
            client_hosts=_get_config_list(config, "ClientHosts"),
            statistics_host=statistics_host,
            port=int(config.get("Port", 22)),
            servers_per_test=_get_config_list(config, "ServersPerTest", int),
            tests_package_path=tests_package_path,
            public_key_path=public_key_path,
            private_key_path=private_key_path,
            password_required=password_required,
            zookeeper_download_url=config["ZookeeperDownloadUrl"],
            zookeeper_hosts=_get_config_list(config, "ZookeeperHosts"),
            kafka_download_url=config["KafkaDownloadUrl"],
            kafka_hosts=_get_config_list(config, "KafkaHosts"),
            full_execution_time=full_execution_time)


def _read_settings_from_java_properties(tests_package_path):
    with zipfile.ZipFile(tests_package_path) as tests_package:
        byte_text = tests_package.read(common.load_test_properties_path)
        return JavaProperties.read(byte_text.decode().splitlines())


def _get_config_list(config, comma_separated_list_name, cls=str):
    return [cls(item.strip()) for item in config[comma_separated_list_name].split(",")]
