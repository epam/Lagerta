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
import pathlib
import shlex
import socket
import time

import bitmath
import paramiko
import redo
from paramiko.util import ClosingContextManager
from plumbum.machines.paramiko_machine import ParamikoMachine

logger = logging.getLogger(__name__)

_reconnect_exceptions = (socket.error, TimeoutError, paramiko.SSHException)

# Command that will be executed before any other remote command, intended
# to property initialize environment variables.
_preflight_command = (
    """if [ -f .bash_profile ]; then . .bash_profile; \
    elif [ -f .bashrc ]; then . .bashrc; fi; \
    PATH=$PATH:$HOME/.local/bin:$HOME/bin ; export PATH"""
)


class SSHClient(ClosingContextManager):
    def __init__(self, host, username, password=None, private_key_path=None,
                 passphrase_required=False, port=22):
        super().__init__()
        self._host = host
        self._username = username
        self._password = password
        self._private_key_path = private_key_path
        self._passphrase_required = passphrase_required
        self._port = port
        self._paramiko_machine = None

    @property
    def host(self):
        return self._host

    @property
    def sftp(self):
        return self._paramiko_machine.sftp

    def connect(self, reconnect=False, timeout=5):
        if self._paramiko_machine is not None:
            if not reconnect:
                return
            self.close()
        if self._paramiko_machine is None:
            self._paramiko_machine = ParamikoMachine(
                host=self._host, password=self._password, user=self._username, connect_timeout=timeout,
                keyfile=self._private_key_path, missing_host_policy=paramiko.AutoAddPolicy())

    def __enter__(self):
        self.connect()
        return self

    def close(self):
        if self._paramiko_machine is not None:
            self._paramiko_machine.close()
            self._paramiko_machine = None

    def upload(self, localpath, remotepath="."):
        self._paramiko_machine.upload(localpath, remotepath)

    def download(self, remote_path, local_path):
        self._paramiko_machine.download(remote_path, local_path)

    def ssh_exec(self, command, sudo=False, in_background=False, get_pty=False,
                 expected_return_code=None, in_separate_shell=False):
        if in_background:
            command = "sh -c {}".format(shlex.quote(command + " &"))
        elif in_separate_shell:
            command = "sh -c {}".format(shlex.quote(command))
        if sudo:
            command = "sudo " + command
        command = _preflight_command + " ; " + command
        with self._paramiko_machine.session(get_pty or sudo) as session:
            _, stdout, stderr = session.run(command, retcode=expected_return_code)
        return (stdout + " " + stderr).strip()

    def sudo_exec(self, command, *args, **kwargs):
        self.ssh_exec(command, *args, sudo=True, **kwargs)

    def check_output(self, command, expected_output, *args, **kwargs):
        matched_message = "Matched"
        execute_and_check_command = "if {} | grep -q {} ; then echo '{}'; fi".format(
            command, shlex.quote(expected_output), matched_message)
        return matched_message in self.ssh_exec(execute_and_check_command, *args, **kwargs)

    def path_exists(self, path):
        found_msg = "Path exists."
        check = "if [ -e {} ]; then echo '{}'; fi".format(path, found_msg)
        return found_msg in self.ssh_exec(check)

    def ssh_copy_id(self, public_key_path):
        if not self.path_exists("~/.ssh"):
            self.ssh_exec("mkdir ~/.ssh && chmod 700 ~/.ssh")
        if not self.path_exists("~/.ssh/authorized_keys"):
            self.ssh_exec("touch ~/.ssh/authorized_keys"
                          " && chmod 600 ~/.ssh/authorized_keys")
        public_key = pathlib.Path(public_key_path).read_text()
        copy_key_command = (""" KEY={};"""
                            """ if [ -z "$(grep "$KEY" ~/.ssh/authorized_keys )" ];"""
                            """ then echo $KEY >> ~/.ssh/authorized_keys; fi""")
        self.ssh_exec(copy_key_command.format(shlex.quote(public_key)))

    def is_process_running(self, process_name):
        stdout = self.ssh_exec("pgrep -c " + process_name)
        return int(stdout) > 0

    def is_java_running(self):
        return self.is_process_running("java")

    def check_file_contains_text(self, text_file, text_to_find):
        contains_message = "Contains"
        check_command = "if grep -q {} {} ; then echo '{}'; fi".format(
            shlex.quote(text_to_find), text_file, contains_message)
        return contains_message in self.ssh_exec(check_command)

    def has_ignite_started(self, log_file):
        if not self.is_java_running() or not self.path_exists(log_file):
            return False
        return self.check_file_contains_text(log_file, "Topology snapshot")

    def stop_process(self, process_name):
        if self.is_process_running(process_name):
            self.sudo_exec("killall -15 " + process_name)
            time.sleep(5)  # Give process some time to finish.
        else:
            return False
        if self.is_process_running(process_name):
            self.sudo_exec("killall -9 " + process_name)
        return True

    def scp(self, remote_path, local_path="."):
        pkey_used = False
        # Purposefully do not try to circumvent passing passphrase to scp while using private key.
        if self._private_key_path is None or self._passphrase_required:
            scp_command = "scp -o StrictHostKeyChecking=no {} {}".format(local_path, remote_path)
            if self._password is not None:
                scp_command = ("sshpass -p {} " + scp_command).format(
                    shlex.quote(self._password))
        else:
            pkey_used = True
            pkey_file = os.path.basename(self._private_key_path)
            self.upload(self._private_key_path)
            self.ssh_exec("chmod 600 " + pkey_file)
            scp_command = "scp -i {} -o StrictHostKeyChecking=no {} {}".format(
                pkey_file, remote_path, local_path)
        downloaded = False
        retries = 0
        while not downloaded and retries < 5:
            self.ssh_exec(scp_command)
            downloaded = self.path_exists(local_path)
            retries += 1
        if pkey_used:
            self._paramiko_machine.sftp.remove(pkey_file)
        return downloaded

    def modify_file(self, path, load_hook, modify_hook, dump_hook):
        with self._paramiko_machine.sftp.open(path, mode="rU") as file:
            content = load_hook(file)
        modify_hook(content)
        # Paramiko's BufferedFile isn't seekable
        # so we should reopen it to overwrite.
        with self._paramiko_machine.sftp.open(path, mode="wU") as file:
            dump_hook(content, file)

    def reboot(self, timeout=60, reconnect_attempts=5):
        command_schedule_time = 10
        self.sudo_exec("shutdown -r -t {}".format(command_schedule_time))
        self.close()
        time.sleep(command_schedule_time)
        retries = 0
        for _ in redo.retrier(sleeptime=timeout, attempts=reconnect_attempts):
            try:
                self.connect()
                return
            except _reconnect_exceptions:
                retries += 1
                logger.info("Trying to connect to %s after reboot issued for the %d time.", self._host, retries)
        raise RuntimeError("Unable to connect to %s after issuing reboot.", self._host)

    def get_available_memory(self, unit=bitmath.Byte):
        free_bytes = int(self.ssh_exec("free -b | gawk  '/Mem:/{print $7}'"))
        return unit(bytes=free_bytes)

    def wget(self, url):
        file_name = url.rsplit("/", 1)[1]
        if self.path_exists(file_name):
            return
        self.ssh_exec("wget " + url)


@redo.retriable(sleeptime=10, retry_exceptions=_reconnect_exceptions)
def ssh_with_config(host, config):
    return SSHClient(host, password=config.password, username=config.username,
                     private_key_path=config.private_key_path, port=config.port,
                     passphrase_required=config.password_required)
