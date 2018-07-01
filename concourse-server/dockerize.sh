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

cd "$(dirname "$0")"
HOME="`pwd -P`"
HOME=`cd ${HOME}; pwd`
cd ..

version=$1

if [ -z "$version" ] || ! [[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+(-.*)?$ ]]; then
  echo "Please provide a valid version number"
  exit 1
else
  # Determine all the docker tags to publish
  major=$(echo ${version} | cut -d . -f 1)
  minor=$(echo ${version} | cut -d . -f 2)
  patch=$(echo ${version} | cut -d . -f 3 | cut -d '-' -f 1)
  suffix=$(echo ${version} | cut -d '-' -f 2-)
  if [[ "$suffix" == "$version" ]]; then
    suffix=""
    tags="latest " # No suffix indicates we should push a latest tag
  else
    suffix="-"$suffix
    tags=""
  fi

  tags="$tags$major$suffix $major.$minor$suffix $major.$minor.$patch$suffix"

  # Build the main docker image
  image=$(date +%Y%m%d%H%M%S)
  docker build -t $image -f Dockerfile .

  # Build the -onbuild image using the image from the previous step as a base
  temp=Dockerfile.onbuild.temp
  sed "1s/.*/FROM $image/" Dockerfile.onbuild > $temp
  docker build -t $image-onbuild -f $temp --pull=false .
  rm $temp

  # Push the image to docker hub with each of the tags
  docker login -u $DOCKER_USER -p $DOCKER_PASS
  for tag in $tags; do
    for type in " " "-onbuild"; do
      name=cinchapi/concourse:$tag$type
      docker tag $image$type $name
      docker push $name
      echo "Pushed $name"
    done
  done
  exit 0
fi