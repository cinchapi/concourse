#!/usr/bin/env bash

# Copyright (c) 2018 Cinchapi Inc.
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

# A script to build and publish docker images for the project.

cd "${0%/*}"
HOME="`pwd -P`"
HOME=`cd ${HOME}; pwd`
cd ..

tag=$1

if [ -z "$tag" ]; then
  echo "Please provide a tag"
  exit 1
else
  # Build the docker image
  docker build -t cinchapi/concourse:$tag -f Dockerfile .

  # Push the docker image to docker hub
  docker login -u $DOCKER_USER -p $DOCKER_PASS
  docker push cinchapi/concourse:$tag

  # TODO: check if tag indicates a master build and push the latest tag
  exit 0
fi