#!/usr/bin/env bash

# Copyright (c) 2015 Cinchapi Inc.
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

# This script will do one time initialization in the repo and delete itself

#Ensure that this script operates from the directory in which it resides
cd "$(dirname "$0")"
this=`basename "$0"`

if [ ! -f gradle.properties ]; then
    printf "sonatypeUsername=\nsonatypePassword=" > gradle.properties
fi
git update-index --assume-unchanged $this
git update-index --assume-unchanged concourse-server/launch/Start\ Concourse.launch
git update-index --assume-unchanged concourse-server/launch/Stop\ Concourse.launch
git update-index --assume-unchanged concourse-shell/launch/Launch\ CaSH.launch
git update-index --assume-unchanged concourse-server/.douge
./utils/install-git-hooks.sh
rm $this
exit 0
