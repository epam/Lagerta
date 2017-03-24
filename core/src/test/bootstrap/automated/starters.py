import abc
import collections
import logging
import posixpath
import time
from functools import partial

import common
from java_properties import JavaProperties
from node_type import NodeType
from ssh_client import ssh_with_config
from util import await_futures

logger = logging.getLogger(__name__)


def recreate_work_dir(ssh_client, path):
    ssh_client.sudo_exec("rm -rf " + path)
    ssh_client.sudo_exec("mkdir -p " + path)
    ssh_client.sudo_exec("chmod -R 777 " + path)


def ensure_enough_memory_available(ssh_client, amount):
    unit = type(amount)
    if ssh_client.get_available_memory(unit) >= amount:
        return
    # Try to clear page cache first.
    ssh_client.sudo_exec("sh -c 'sync; echo 1 > /proc/sys/vm/drop_caches'")
    if ssh_client.get_available_memory(unit) >= amount:
        return
    # Attempt to reboot a node as there are known memory leaks with our test
    # probably related to the Ignite, when there is no process displayed
    # who takes the memory but memory is consumed.
    logger.info("Rebooting %s to free RAM.", ssh_client.host)
    ssh_client.reboot()
    if not ssh_client.get_available_memory(unit) >= amount:
        raise RuntimeError("unable to free enough memory on {}".format(ssh_client.host))


# ToDo: currently there is no way to interrupt a Starter while it is waiting
# for start of the process.
class Starter(object):
    def __init__(self, config):
        super().__init__()
        self._config = config

    @abc.abstractmethod
    def start(self):
        pass

    def _prestart_common(self, ssh_client):
        node_type = self._config.determine_node_type(ssh_client.host)
        recreate_work_dir(ssh_client, node_type.work_dir)
        ensure_enough_memory_available(ssh_client, node_type.required_available_memory)
        self._start_gmond_sender(ssh_client)

    def _start_gmond_sender(self, ssh_client):
        if not ssh_client.is_process_running("gmond"):
            gmond_config_path = posixpath.join(self._config.remote_home, "gmond.conf")
            ssh_client.sudo_exec("gmond --conf={}".format(gmond_config_path))
            logger.info("Started gmond sender on %s.", ssh_client.host)

    def _wait_till_started(self, host, process_name, check_started):
        startup = time.time()
        sleep_time = max(common.start_timeout / 10, 5)  # To not poll too often.
        start_time_expiration_reported = False
        while True:
            if check_started():
                logger.info("%s started on %s.", process_name, host)
                return
            if not start_time_expiration_reported:
                passed_time = time.time() - startup
                if passed_time > common.start_timeout:
                    logger.warning("%s on node %s not started in %f seconds."
                                   " Continue waiting.", process_name, host, common.start_timeout)
                    start_time_expiration_reported = True
            time.sleep(sleep_time)


class IgniteStarter(Starter):
    __start_command_pattern = (
        "cd {testdir}; {remote_home}/activestore-tests/{start_script} > ignite.log")

    def __init__(self, config, start_script):
        super().__init__(config=config)
        self._start_script = start_script

    @abc.abstractmethod
    def start(self):
        pass

    def _start(self, host, test_name):
        with ssh_with_config(host, self._config) as ssh_client:
            self._prestart_common(ssh_client)
            testdir = posixpath.join(self._config.remote_home, test_name)
            if ssh_client.path_exists(testdir):
                # Backup files from previous runs if exist.
                timestamp = int(time.time())
                ssh_client.sudo_exec("mv {0} {0}_{1}".format(testdir, timestamp))
            ssh_client.ssh_exec("mkdir -p " + testdir)
            start_command = IgniteStarter.__start_command_pattern.format(
                testdir=testdir, remote_home=self._config.remote_home,
                start_script=self._start_script)
            self._do_before_start(ssh_client, testdir)
            ssh_client.ssh_exec(start_command, in_background=True)
            log_file = posixpath.join(testdir, "ignite.log")
            check_started = partial(ssh_client.has_ignite_started, log_file=log_file)
            self._wait_till_started(host, "Ignite", check_started)

    def _do_before_start(self, ssh_client, testdir):
        pass


class StatisticsClientStarter(IgniteStarter):
    def __init__(self, config, test_name):
        super().__init__(config=config, start_script="start-statistics-driver.sh")
        self._test_name = test_name

    def start(self):
        self._start(host=self._config.statistics_host,
                    test_name=self._test_name)

    def _do_before_start(self, ssh_client, testdir):
        remote_reportdir = posixpath.join(testdir, common.reportdir_name)
        ssh_client.ssh_exec("mkdir -p " + remote_reportdir)
        properties_path = posixpath.join(self._config.remote_home, common.load_test_properties_path)
        modify_properties_hook = partial(self._modify_reportdir_location_property,
                                         reportdir_location=remote_reportdir)
        ssh_client.modify_file(properties_path, JavaProperties.read, modify_properties_hook, JavaProperties.dump)

    def _modify_reportdir_location_property(self, properties, reportdir_location):
        properties["load.tests.statistics.aggregated.report.location"] = reportdir_location
        properties["load.tests.statistics.ganglia.port"] = NodeType.STATISTICS_HOST.gmond_port


class IgniteServersStarter(IgniteStarter):
    def __init__(self, config, test_name, server_number, executor):
        super().__init__(config=config, start_script="start-ignite.sh")
        self._test_name = test_name
        self._server_number = server_number
        self._executor = executor

    def start(self):
        await_futures(self._executor.submit(self._start, server, self._test_name)
                      for server in self._config.server_hosts[:self._server_number])
        # Stop gmond on remaining servers to have cleaner Ganglia statistics.
        await_futures(self._executor.submit(self._ensure_gmond_stopped, server, self._config)
                      for server in self._config.server_hosts[self._server_number:])
        # Wait some time to ensure cluster is built up.
        time.sleep(common.start_timeout)

    def _ensure_gmond_stopped(self, host, config):
        with ssh_with_config(host, config) as ssh_client:
            ssh_client.stop_process("gmond")
            logger.info("Stopped gmond sender on %s unused in current test.", host)


class TestClientsStarter(IgniteStarter):
    def __init__(self, config, test_name, executor):
        super().__init__(config=config, start_script="simulation-load-test.sh")
        self._test_name = test_name
        self._executor = executor
        self._scheduling = False

    def start(self):
        self._start_first_client_and_wait_for_prepopulate()
        await_futures(self._executor.submit(self._start, host, self._test_name)
                      for host in self._config.client_hosts[1:])

    def _start_first_client_and_wait_for_prepopulate(self):
        host = self._config.client_hosts[0]
        self._start(host, self._test_name)
        log_file = posixpath.join(self._config.remote_home, self._test_name, "ignite.log")
        prepopulation_start_time = time.time()
        timeout_expiration_reported = False
        logger.info("Waiting for the first client to prepopulate caches.")
        while True:
            with ssh_with_config(host, self._config) as ssh_client:
                text_to_find = "Setting up load tests driver"
                if ssh_client.check_file_contains_text(log_file, text_to_find):
                    logger.info("Prepopulation finished.")
                    return
            if not timeout_expiration_reported:
                passed_time = time.time() - prepopulation_start_time
                if passed_time > common.prepopulation_timeout:
                    logger.warning("Caches prepopulation from node %s not completed in %.2f seconds."
                                   " Continue waiting.", host, common.prepopulation_timeout)
                    timeout_expiration_reported = True
            time.sleep(common.test_running_poll_time)


class ZookeeperClusterStarter(Starter):
    def __init__(self, config, executor):
        super().__init__(config=config)
        self._executor = executor

    def start(self):
        await_futures(self._executor.submit(self._start_server, host)
                      for host in self._config.zookeeper_hosts)
        time.sleep(common.start_timeout)

    def _start_server(self, host):
        host_index = self._config.zookeeper_hosts.index(host) + 1
        start_command = "{}/bin/zkServer.sh start".format(
            self._config.zookeeper_dir_name)
        with ssh_with_config(host, self._config) as ssh_client:
            self._prestart_common(ssh_client)
            ssh_client.ssh_exec("echo '{}' > {}/myid".format(
                host_index, common.zookeeper_data_dir))
            ssh_client.ssh_exec(start_command)
            check_started = partial(self._check_started, ssh_client=ssh_client,)
            self._wait_till_started(host, "Zookeeper", check_started)

    def _check_started(self, ssh_client):
        status_command = "{}/bin/zkServer.sh status".format(self._config.zookeeper_dir_name)
        return not ssh_client.check_output(status_command, "Error contacting service")


class KafkaClusterStarter(Starter):
    def __init__(self, config, executor):
        super().__init__(config=config)
        self._executor = executor

    def start(self):
        await_futures(self._executor.submit(self._start_server, host)
                      for host in self._config.kafka_hosts)
        time.sleep(common.start_timeout)

    def _start_server(self, host):
        start_command = (
            "{0}/bin/kafka-server-start.sh"
            " {0}/config/server.properties > kafka.log").format(
                self._config.kafka_dir_name)
        with ssh_with_config(host, self._config) as ssh_client:
            self._prestart_common(ssh_client)
            ssh_client.ssh_exec(start_command, in_background=True)
            check_started = partial(KafkaClusterStarter._check_started, ssh_client=ssh_client)
            self._wait_till_started(host, "Kafka", check_started)

    @staticmethod
    def _check_started(ssh_client):
        if not ssh_client.is_java_running():
            return False
        text_to_find = "starting (kafka.server.KafkaServer)"
        return ssh_client.check_file_contains_text("kafka.log", text_to_find)


class _StartHelper(collections.abc.Callable):
    def __init__(self, starter_class):
        super().__init__()
        self._starter_class = starter_class

    def __call__(self, *args, **kwargs):
        starter = self._starter_class(*args, **kwargs)
        starter.start()


start_statistics_client = _StartHelper(StatisticsClientStarter)

start_ignite_servers = _StartHelper(IgniteServersStarter)

start_test_clients = _StartHelper(TestClientsStarter)

start_zookeeper_cluster = _StartHelper(ZookeeperClusterStarter)

start_kafka_cluster = _StartHelper(KafkaClusterStarter)
