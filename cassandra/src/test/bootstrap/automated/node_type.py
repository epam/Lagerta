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

class NodeType(object):
    def __init__(self, cluster_name, required_available_memory, work_dir, gmond_port, start_timeout=30):
        super().__init__()
        self._cluster_name = cluster_name
        self._work_dir = work_dir
        self._required_available_memory = required_available_memory
        self._gmond_port = gmond_port
        self._start_timeout = start_timeout

    @property
    def cluster_name(self):
        return self._cluster_name

    @property
    def work_dir(self):
        return self._work_dir

    @property
    def required_available_memory(self):
        return self._required_available_memory

    @property
    def gmond_port(self):
        return self._gmond_port

    @property
    def start_timeout(self):
        return self._start_timeout
