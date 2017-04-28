#!/bin/bash

mkdir -p /var/run/sshd
sed -i "s/UsePrivilegeSeparation.*/UsePrivilegeSeparation no/g" /etc/ssh/sshd_config
sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config
touch /root/.Xauthority true

chmod 0755 /var/run/sshd
/usr/sbin/sshd -D