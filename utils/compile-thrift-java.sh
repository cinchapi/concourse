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

# Compile each module and cleanup the generated source code
for module in "${MODULES[@]}"; do
  service=$(cat $module | grep service | cut -d ' ' -f 2)
  # Run the thrift compile
  thrift -out $TARGET -gen java $module

  if [ $? -ne 0 ]; then
    exit 1
  fi

  # Replace all instances of Hash* data structures with their LinkedHash
  # counterparts (see THRIFT-2115)
  perl -p -i -e "s/Hash/LinkedHash/g" $PACKAGE"/$service.java"
  
  # Remove all existing @SuppressWarnings annotations
  sed -i '' -e '/@SuppressWarnings/d' $PACKAGE"/$service.java"

  # Add the specific @SuppressWarnings before the outermost class definition
  suppress_warnings_annotation="@SuppressWarnings({ \"unchecked\", \"rawtypes\", \"serial\", \"unused\" })" 
  awk -v annotation="$suppress_warnings_annotation" '/public class '"$service"'/ { print annotation; } { print }' $PACKAGE"/$service.java" > $PACKAGE"/$service.java.tmp" && mv $PACKAGE"/$service.java.tmp" $PACKAGE"/$service.java"

  echo "Finished compiling the $service API for Java to "$(cd $PACKAGE && pwd)
done

 # Remove unnecessary files
rm $PACKAGE"/concourseConstants.java"

# Generate the StatefulConcourseService class
cd $HOME
SOURCE_DESTINATION=$HOME/../concourse-plugin-core/src/main/java/com/cinchapi/concourse/server/plugin/StatefulConcourseService.java
GENERATOR=$HOME/codegen/StatefulConcourseServiceGenerator.groovy
THRIFT_IDLS=""
for module in "${MODULES[@]}"; do
  THRIFT_IDLS="$THRIFT_IDLS $HOME/../interface/$module"
done
groovy $GENERATOR $THRIFT_IDLS $SOURCE_DESTINATION

echo "Finished generating $SOURCE_DESTINATION"

# Generate the ConcourseManagementService class
cd $HOME
THRIFT_IDL=$HOME/../interface/management/management.thrift
SOURCE_DESTINATION=$HOME/../concourse-server/src/main/java
thrift -out $SOURCE_DESTINATION -gen java $THRIFT_IDL

CONCOURSE_MANAGEMENT_SERVICE_FILE=$SOURCE_DESTINATION/com/cinchapi/concourse/server/management/ConcourseManagementService.java
sed -i '' -e '/@SuppressWarnings/d' $CONCOURSE_MANAGEMENT_SERVICE_FILE
awk -v annotation="$suppress_warnings_annotation" '/public class ConcourseManagementService/ { print annotation; } { print }' $CONCOURSE_MANAGEMENT_SERVICE_FILE > $CONCOURSE_MANAGEMENT_SERVICE_FILE.tmp && mv $CONCOURSE_MANAGEMENT_SERVICE_FILE.tmp $CONCOURSE_MANAGEMENT_SERVICE_FILE

echo "Finished generating $SOURCE_DESTINATION/com/cinchapi/concourse/server/management/ConcourseManagementService.java"

exit 0
