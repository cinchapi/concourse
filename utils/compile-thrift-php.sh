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

# Compile the thrift API for the PHP service

. "`dirname "$0"`/.compile-thrift-include"

TARGET="../concourse-driver-php"
PACKAGE=$TARGET"/src"

cd $THRIFT_DIR

# Run the thrift compile
for module in "${MODULES[@]}"; do
  service=$(cat $module | grep service | cut -d ' ' -f 2)
  thrift -out $PACKAGE -gen php $module

  if [ $? -ne 0 ]; then
    exit 1
  fi

  API=$PACKAGE"/concourse/thrift/$service.php"

  # Replace all TransactionToken declarations to take no type since we allow that
  # parameter to legally be null in method invocations.
  perl -p -i -e "s/, \\\\concourse\\\\thrift\\\\shared\\\\TransactionToken/, /g" $API

  # Ensure that we consistently populate array values instead of array keys
  # regardless of whether the value is scalar or not
  perl -p -i -e 's/\[(\$[A-Za-z]{1,}[0-9]{0,})\]\s*=\strue;/ []= $1;/g' $API

  # Serialize any array keys that are not strings or integers since PHP doesn't
  # allow them.
  perl -p -i -e 's/\[(\$[A-Za-z]{1,}[0-9]{0,})\]/[(!is_string($1) && !is_integer($1)) ? serialize($1) : $1]/g' $API

  echo "Finished compiling the Thrift API for PHP to "$(cd $PACKAGE && pwd)
done

exit 0
