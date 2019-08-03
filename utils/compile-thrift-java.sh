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

# Compile the thrift API for the Java service

. "`dirname "$0"`/.compile-thrift-include"

TARGET="../concourse-driver-java/src/main/java"
PACKAGE=$TARGET"/com/cinchapi/concourse/thrift"

HOME=$THRIFT_DIR/../utils
cd $THRIFT_DIR

# Run the thrift compile
thrift -out $TARGET -gen java concourse.thrift

if [ $? -ne 0 ]; then
  exit 1
fi

# Replace all instances of Hash* data structures with their LinkedHash
# counterparts (see THRIFT-2115)
perl -p -i -e "s/Hash/LinkedHash/g" $PACKAGE"/ConcourseService.java"

# Remove unnecessary files
rm $PACKAGE"/concourseConstants.java"

# Supress all the necessary warnings
perl -p -i -e 's/"cast", "rawtypes", "serial", "unchecked"/"cast", "rawtypes", "serial", "unchecked", "unused"/g' $PACKAGE"/ConcourseService.java"

echo "Finished compiling the Thrift API for Java to "$(cd $PACKAGE && pwd)

# Generate the StatefulConcourseService class
cd $HOME
THRIFT_IDL=$HOME/../interface/concourse.thrift
SOURCE_DESTINATION=$HOME/../concourse-plugin-core/src/main/java/com/cinchapi/concourse/server/plugin/StatefulConcourseService.java
GENERATOR=$HOME/codegen/StatefulConcourseServiceGenerator.groovy

groovy $GENERATOR $THRIFT_IDL $SOURCE_DESTINATION

echo "Finished generating $SOURCE_DESTINATION"

# Generate the ConcourseManagementService class
cd $HOME
THRIFT_IDL=$HOME/../interface/management/management.thrift
SOURCE_DESTINATION=$HOME/../concourse-server/src/main/java
thrift -out $SOURCE_DESTINATION -gen java $THRIFT_IDL

echo "Finished generating $SOURCE_DESTINATION/com/cinchapi/concourse/server/management/ConcourseManagementService.java"

exit 0
