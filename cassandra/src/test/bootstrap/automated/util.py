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

import collections.abc
import logging
import os
import platform
import subprocess

from ssh_client import ssh_with_config

logger = logging.getLogger(__name__)


def await_futures(futures):
    # Conversion to list is necessary in order to instantiate all futures
    # before waiting for completion in case the generator expression was passed,
    # otherwise the execution will be equivalent to sequential in that case.
    futures = list(futures)
    for future in futures:
        future.result()


def get_script_workdir():
    return os.path.dirname(os.path.realpath(__file__))


def terminate(process, kill_timeout=5):
    if "Windows" in platform.system():
        # Simply calling `terminate` may leave program hanging as on Windows
        # it does not necessarily lead to termination of the whole process tree.
        subprocess.run("taskkill /F /T /PID " + str(process.pid), timeout=kill_timeout,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    else:
        process.kill()


def stop_java_forcibly(host, config, expected_stopped_process_name):
    with ssh_with_config(host, config) as ssh_client:
        if ssh_client.stop_process("java"):
            logger.info("%s on %s was stopped forcibly.",
                        expected_stopped_process_name, host)


def stop_javas_forcibly(hosts, config, executor, expected_stopped_process_name):
    await_futures(executor.submit(stop_java_forcibly, host, config,
                                  expected_stopped_process_name)
                  for host in hosts)


class lazy_property(object):
    def __init__(self, initializer):
        self._initializer = initializer
        self._value = None

    def __get__(self, obj, cls=None):
        if obj is None:
            return None
        if self._value is None:
            self._value = self._initializer(obj)
        return self._value


class TypedSimpleNamespace(collections.abc.Mapping):
    """Similar to :class:`types.SimpleNamespace`, expects values of kwargs
    to be a tuple-like, sets attributes creating new objects of passed type
    using unpacked values as its constructor arguments.
    """

    def __init__(self, cls, **kwargs):
        super().__init__()
        for key, value in kwargs.items():
            setattr(self, key, cls(*value))

    def __getitem__(self, name):
        return getattr(self, name)

    def __len__(self):
        return len(self.__dict__)

    def __iter__(self):
        return iter(self.__dict__)
