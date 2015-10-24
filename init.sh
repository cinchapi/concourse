#!/usr/bin/env bash
# This script will do one time initialization in the repo and delete itself

#Ensure that this script operates from the directory in which it resides
cd "$(dirname "$0")"
this=`basename "$0"`

printf "sonatypeUsername=\nsonatypePassword=" > gradle.properties
git update-index --assume-unchanged $this
git update-index --assume-unchanged concourse-server/launch/Start\ Concourse.launch
git update-index --assume-unchanged concourse-server/launch/Stop\ Concourse.launch
git update-index --assume-unchanged concourse-shell/launch/Launch\ CaSH.launch
./utils/install-git-hooks.sh
rm $this
exit 0
