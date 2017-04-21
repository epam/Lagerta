#
# Copyright (c) 2017. EPAM Systems
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import os
import platform
import posixpath
import subprocess
import time
import zipfile
from tempfile import TemporaryDirectory

import click
from futurist import ThreadPoolExecutor

import common
from cleaner import cleanup_cluster
from configuration import Configuration
from preparation import prepare_cluster
from ssh_client import ssh_with_config
from starters import (start_statistics_client, start_ignite_servers, start_test_clients,
                      start_zookeeper_cluster, start_kafka_cluster)
from util import get_script_workdir, stop_java_forcibly, stop_javas_forcibly, terminate

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(10)


def try_call_cluster_stopper(config):
    try:
        with TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(config.tests_package_path) as tests_package:
                tests_package.extractall(temp_dir)
            stopper_script = os.path.join(temp_dir, "activestore-tests", "stop-cluster")
            if "Windows" in platform.system():
                stopper_script += ".bat"
            else:
                stopper_script += ".sh"
            logger.info("Shutting down the cluster using Ignite Cluster API.")
            process = subprocess.Popen(stopper_script, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                process.wait(common.cluster_stopper_timeout)
                logger.info("Shutdown procedure finished.")
            except subprocess.TimeoutExpired:
                logger.warning("Shutdown command was unable to complete.")
                terminate(process)
    except Exception:
        logger.warning("Exception when executing cluster stopper script.", exc_info=True)


def is_statistics_driver_finished(config, test_name,):
    with ssh_with_config(config.statistics_host, config) as ssh_client:
        if not ssh_client.is_java_running():
            return True
        driver_log_path = posixpath.join(config.remote_home, test_name,  "ignite.log")
        cluster_stopped_line = "Cluster is considered overloaded and will be stopped."
        return ssh_client.check_file_contains_text(driver_log_path, cluster_stopped_line)


def await_test_termination(config, test_name):
    test_running = True
    # Expected full test execution time.
    soft_timeout = config.full_execution_time
    # Time after which the test will be forcibly stopped if still running.
    hard_timeout = 1.1 * soft_timeout
    test_start_time = time.time()
    while test_running:
        time_passed = time.time() - test_start_time
        if time_passed > hard_timeout:
            logger.warning("Expected execution time exceeded by more than 10 percents.")
            logger.warning("Test considered hanged.")
            break
        logger.info("Test running for %.2f/%.2f seconds.", time_passed, soft_timeout)
        if time_passed > soft_timeout:
            logger.warning("Predefined test run time exceeded by %.2f seconds", time_passed - soft_timeout)
        if is_statistics_driver_finished(config, test_name):
            test_running = False
            logger.info("Test finished by itself.")
            # Do not break here to give cluster some time to stop.
        time.sleep(common.test_running_poll_time)
    if test_running:
        try_call_cluster_stopper(config)
    logger.info("Forcibly stopping remaining Ignite instances on each of cluster nodes.")
    # The order here is important at first shutdown clients as they send computes to servers
    # and statistics info to statistics host. Then the servers as they may send computes
    # with statistics info to statistics host. Statistics host only gathers statistics info
    # thus it is stopped the last.
    stop_javas_forcibly(config.client_hosts, config, _executor, "Ignite")
    stop_javas_forcibly(config.server_hosts, config, _executor, "Ignite")
    stop_java_forcibly(config.statistics_host, config, "Ignite")

    logger.info("Stopping zookeeper/kafka cluster.")
    stop_javas_forcibly(config.kafka_hosts, config, _executor, "Kafka")
    stop_javas_forcibly(config.zookeeper_hosts, config, _executor, "Zookeeper")


def download_statistics_report(config, test_name):
    reportdir = os.path.join(get_script_workdir(), test_name)
    remote_reportdir = posixpath.join(config.remote_home, test_name, common.reportdir_name)
    if os.path.exists(reportdir):
        timestamp = int(time.time())
        os.rename(reportdir, reportdir + "_" + str(timestamp))
    os.makedirs(reportdir)
    with ssh_with_config(config.statistics_host, config) as ssh_client:
        ssh_client.download(remote_reportdir, reportdir)
    logger.info("Downloaded statistics reports to %s.", reportdir)


def run_load_tests(config, test_name_prefix, redeploy_activestore):
    prepare_cluster(config, redeploy_activestore, _executor)
    for server_number in config.servers_per_test:
        server_noun = "server" if server_number == 1 else "servers"
        logger.info("Starting test for %d %s.", server_number, server_noun)
        logger.info("Starting zookeeper/kafka cluster.")
        start_zookeeper_cluster(config, _executor)
        start_kafka_cluster(config, _executor)

        test_name = common.test_name_pattern.format(test_name_prefix, server_number)
        logger.info("Starting ignite servers.")
        start_ignite_servers(config, test_name, server_number, _executor)
        logger.info("Starting statistics host.")
        start_statistics_client(config, test_name)
        logger.info("Running test load clients.")
        start_test_clients(config, test_name, _executor)

        await_test_termination(config, test_name)
        download_statistics_report(config, test_name)
        logger.info("Finished test for %d %s.", server_number, server_noun)
    cleanup_cluster(config, _executor)


@click.command()
@click.option("--config", "config_path", default="config.ini", help="Configuration file.")
@click.option("--test-name-prefix", default="scal", help="String to be used to prefix"
              " test working directories on remote hosts.")
@click.option("--redeploy", "redeploy_activestore", is_flag=True)
def cli(config_path, test_name_prefix, redeploy_activestore):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.StreamHandler()])
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    config = Configuration.read(config_path)
    run_load_tests(config, test_name_prefix, redeploy_activestore)


if __name__ == "__main__":
    cli()
