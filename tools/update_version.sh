#!/bin/bash

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

# example `sh update_version.sh 0.5.0.0 0.6.0.0-SNAPSHOT`

OLD=$1 # "0.5.0.0"
NEW=$2 # "0.6.0.0-SNAPSHOT"

HERE=` basename $PWD`
if [[ ${HERE} != "tools" ]]; then
    echo "Please only execute in the tools/ directory";
    exit 1;
fi

echo Updating version from ${OLD} to ${NEW}

find .. -name 'pom.xml' -type f -exec sed -i '/<parent>/,/<\/parent>/ s|<version>'${OLD}'</version>|<version>'${NEW}'</version>|g' {} \;
sed -i 's|<version>'${OLD}'</version>|<version>'${NEW}'</version>|g' ../pom.xml
find .. -name 'project.clj' -type f -exec sed -i 's|lagerta-jepsen "'${OLD}'"|lagerta-jepsen "'${NEW}'"|g' {} \;
