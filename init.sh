#!/usr/bin/env bash
# This script will do one time initialization in the repo and delete itself

printf "sonatypeUsername=\nsonatypePassword=" > gradle.properties
git update-index --assume-unchanged $0
git update-index --assume-unchanged concourse-server/launch/Start\ Concourse.launch
git update-index --assume-unchanged concourse-server/launch/Stop\ Concourse.launch
git update-index --assume-unchanged concourse-shell/launch/Launch\ CaSH.launch
./utils/install-git-hooks.sh
rm $0
exit 0
