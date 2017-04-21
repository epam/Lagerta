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

from bitmath import GiB

from node_type import NodeType
from util import TypedSimpleNamespace

# Relative name of the directory where statistics report csv files
# will be stored on the statistics node.
reportdir_name = "report"

test_name_pattern = "{}-load-test-{}s"

# Path to load tests properties file of the testing package relative
# to the home directory of a user used to login to remote hosts.
load_test_properties_path = "activestore-tests/settings/load-tests.properties"

ganglia_host_property_name = "load.tests.statistics.ganglia.host"

# Time in seconds to wait until node is considered to be started.
start_timeout = 30

# Time in seconds used to poll test to check if test is still running.
test_running_poll_time = 30

# Maximum time to wait for initial prepopulation of caches.
prepopulation_timeout = 5400

# Time to wait for cluster stopping script after which it will be terminated
# and remaining cluster nodes will be stopped forcibly.
# Note: cluster is always checked for remaining nodes which are stopped
#       even if stopper script completes in time.
cluster_stopper_timeout = 180

# Directory that will be created and set as Zookeeper data directory for each Zookeeper server.
zookeeper_data_dir = "/var/zookeeper/data"

# Directory that will be created and set for storing logs for each Kafka broker.
kafka_logs_dir = "/var/kafka/logs"

# Directory that will be created and set as Ignite work directory for each Ignite client/server
# node. It is a different directory from the one where script starts servers/clients/statistics host.
ignite_work_dir = "/tmp/ignite"

# This map will be used to overwrite default properties in kafka
# broker configuration on kafka nodes.
kafka_properties = {
    "log.flush.scheduler.interval.ms": 20000,
    "num.partitions": 32,
    "log.dirs": kafka_logs_dir
}

# This map will be used to overwrite default properties in zookeeper
# configuration on zookeeper nodes.
zookeeper_properties = {
    "dataDir": zookeeper_data_dir
}

gmetad_echo_source_line_pattern = (
    r"""echo "data_source \"{cluster_name}\" {hostname}:{port}" >> {config}"""
)

gmetad_config_base_edits = [r"""echo "setuid_username \"ganglia\"" >> {config} """,
                            r"""echo "case_sensitive_hostnames 0" >> {config} """]

# Some default settings that make gmond work in unicast mode and adjust other settings.
# Metadata send interval is set here to 30 seconds, though in most of cases it can
# be safely increased to 60 seconds, further increase may lead to seeing hosts down
# at web interface and leaving this value to 0 may lead to seeing no graphs.
generic_gmond_config_sed_pattern = (
    r"""cat {default_config} | \
    sed -r "s/name = \"unspecified\"/name = \"{cluster_name}\"/g" | \
    sed -r "s/#bind_hostname/bind_hostname/g" | \
    sed -r "s/send_metadata_interval = 0/send_metadata_interval = 60/g" | \
    sed "0,/mcast_join = 239.2.11.71/s/mcast_join = 239.2.11.71/host = {hostname}/g" | \
    sed -r "s/mcast_join = 239.2.11.71//g" | sed -r "s/bind = 239.2.11.71//g" | \
    sed -r "s/port = 8649/port = {port}/g" | sed -r "s/retry_bind = true//g" """
)

gmond_sender_config_sed_pattern = generic_gmond_config_sed_pattern + \
    r""" | sed -r "s/deaf = no/deaf = yes/g" """

gmond_receiver_config_sed_pattern = generic_gmond_config_sed_pattern + \
    r""" | sed -r "s/mute = no/mute = yes/g" | sed -r "s/host_dmax = 86400/host_dmax = 300/g" """

# Will be used to replace /etc/httpd/conf.d/ganglia.conf on ganglia master node.
httpd_ganglia_conf = (
    "Alias /ganglia /usr/share/ganglia\n"
    "<Location /ganglia>\n"
    "    Allow from all\n"
    "    Deny from none\n"
    "    Require all granted\n"
    "</Location>\n"
)

# Period in seconds at which atop will record node metrics locally.
atop_report_period = 30

# Node logs (ignite, kafka, atop, etc.) older than this number of days will be removed.
logs_retention_days = 5

# Node types that may appear in load test.
node_types = TypedSimpleNamespace(
    cls=NodeType,
    IGNITE_SERVER=("ignite-server", GiB(6), ignite_work_dir, 8641),
    IGNITE_CLIENT=("ignite-client", GiB(2), ignite_work_dir, 8642),
    STATISTICS_HOST=("statistics-driver", GiB(2), ignite_work_dir, 8643),
    ZOOKEEPER_SERVER=("zookeeper", GiB(2), zookeeper_data_dir, 8644),
    KAFKA_BROKER=("kafka", GiB(6), kafka_logs_dir, 8645)
)


# Common prepare step specific for you test case, like disabling root crontab
# to avoid cpu-heavy jobs running during load test execution.
def specific_prepare_hook(ssh_client):
    ssh_client.sudo_exec("crontab -l > cron.bak")
    ssh_client.sudo_exec("crontab -r")


# Common cleanup step after tests specific for you test case.
def specific_cleanup_hook(ssh_client):
    ssh_client.sudo_exec("crontab cron.bak")
    ssh_client.sudo_exec("rm -f cron.bak")
