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

# Run the tasks to generate all the driver docs
. "`dirname "$0"`/.include"

# Java
cd ..
./gradlew javadoc

# Python
cd concourse-driver-python
./pyinvoke clean docs
cd ..

# PHP
cd concourse-driver-php
./phake clean docs
cd ..

# Ruby
cd concourse-driver-ruby
./rake clean docs
cd ..

exit 0
