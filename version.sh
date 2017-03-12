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

#################################################
###  Script to get or set the current version ###
#################################################

#Ensure that this script operates from the directory in which it resides
cd "$(dirname "$0")"

BASE_VERSION_FILE=".version"
JENKINS_HOME="/opt/jenkins"
if [ -w $JENKINS_HOME ]; then
  COUNTER_FILE="$JENKINS_HOME/.counter"
else
  COUNTER_FILE=".counter"
fi

if [ -z "$1" ] ; then
  VERSION=`cat $BASE_VERSION_FILE`
  BRANCH=`git rev-parse --abbrev-ref HEAD`
  COMMIT=`git rev-parse HEAD | cut -c1-10`

  # Get counter value
  if [ ! -f $COUNTER_FILE ] ; then
    echo 0 > $COUNTER_FILE
  fi
  if [ -z ${CONTAINER_BUILD+x} ]; then
    COUNTER=`cat $COUNTER_FILE`
    ((COUNTER++))
  else
    # If the build is run in a container, the COUNTER_FILE will be wiped
    # away on each build, so we'll use the current unix timestamp for the
    # counter.
    COUNTER=`date +%s`
  fi
  echo $COUNTER > $COUNTER_FILE
  VERSION=$VERSION.$COUNTER
  case $BRANCH in
    develop )
      EXTRA="-SNAPSHOT"
      ;;
    feature* )
      IFS='/'
      PARTS=( $BRANCH )
      EXTRA="-${PARTS[1]}"
      EXTRA=`echo $EXTRA | tr '[:lower:]' '[:upper:]'`
      ;;
    release* )
      EXTRA=""
      ;;
    * )
      # At this point we do not need to refer
      # to any commit hash since we'll have a
      # tag that represents the overall release
      EXTRA=""
      ;;
  esac
  echo $VERSION$EXTRA
else
  NEW_VERSION=$1
  if [[ $NEW_VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] ; then
    echo $NEW_VERSION > $BASE_VERSION_FILE
    rm $COUNTER_FILE 2>/dev/null
    sed -i '' -E "s/[0-9]+\.[0-9]+\.[0-9]+/$NEW_VERSION/g" README.md

    # Find all the build.gradle files and update the version if it is
    # listed.
    files=( `find . -name "build.gradle" | cut -d / -f 2-` )
    for file in "${files[@]}"
    do
      sed -i '' -E "s/pom.version = '[0-9]+\.[0-9]+\.[0-9]'+/pom.version = '$NEW_VERSION'/g" $file
      sed -i '' -E "s/pom.version = '[0-9]+\.[0-9]+\.[0-9]-SNAPSHOT'+/pom.version = '$NEW_VERSION-SNAPSHOT'/g" $file
    done

    # concourse-driver-java
    sed -i '' -E "s/[0-9]+\.[0-9]+\.[0-9]+/$NEW_VERSION/g" concourse-driver-java/README.md

    # concourse-driver-php
    sed -i '' -E "s/[0-9]+\.[0-9]+\.[0-9]+/$NEW_VERSION/g" concourse-driver-php/README.md
    sed -i '' -E "s/\"version\": \"[0-9]+\.[0-9]+\.[0-9]+\"/\"version\": \"$NEW_VERSION\"/g" concourse-driver-php/composer.json

    # concourse-driver-python
    sed -i '' -E "s/version='[0-9]+\.[0-9]+\.[0-9]'+/version='$NEW_VERSION'/g" concourse-driver-python/README.md

    echo "The version has been set to $NEW_VERSION"
  else
    echo "Please specify a valid version <major>.<minor>.<patch>"
    exit 1
  fi
fi
exit 0
