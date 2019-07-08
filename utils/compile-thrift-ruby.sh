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

# Compile the thrift API for the Ruby service

. "`dirname "$0"`/.compile-thrift-include"

TARGET="../concourse-driver-ruby/lib/"
PACKAGE=$TARGET

cd $THRIFT_DIR

# Run the thrift compile
for module in "${MODULES[@]}"; do
  service=$(cat $module | grep service | cut -d ' ' -f 2)
  thrift -out $TARGET -gen rb:namespaced $module

  if [ $? -ne 0 ]; then
    exit 1
  fi

  echo "Finished compiling the Thrift API for Ruby to "$(cd $PACKAGE && pwd)
done

exit 0
