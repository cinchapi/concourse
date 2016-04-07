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

# Compile the thrift API for the all languages

. "`dirname "$0"`/.compile-thrift-include"

ME=`basename $0`
cd $THRIFT_DIR"/../utils"
for SCRIPT in `ls | grep compile-thrift`
do
  if [ -f $SCRIPT -a $SCRIPT != $ME ];
    then
    echo
    bash $SCRIPT
  fi
done
