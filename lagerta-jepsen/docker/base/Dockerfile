################################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
################################################################################

FROM openjdk:8

LABEL "base docker image with java 8"

# install initial deps
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y apt-utils
RUN apt-get update && apt-get -y upgrade && \
  apt-get install -y --no-install-recommends \
  libjna-java \
  net-tools \
  sysvinit \
  sysvinit-core \
  sysvinit-utils \
  ssh \
  faketime \
  iptables \
  iputils-ping \
  logrotate \
  gnuplot \
  vim \
  netcat \
  software-properties-common && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

RUN apt-get remove -y --purge --auto-remove systemd

# configure ssh configs
RUN mkdir -p /var/run/sshd && \
    sed -i "s/UsePrivilegeSeparation.*/UsePrivilegeSeparation no/g" /etc/ssh/sshd_config && \
    sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    touch /root/.Xauthority true

RUN chmod 755 /var/run/sshd
ARG PASSWORD=root
RUN echo 'root:'${PASSWORD} | chpasswd

EXPOSE 22
