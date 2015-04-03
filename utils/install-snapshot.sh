#!/usr/bin/env bash
# A script to install the latest development snapshot

. "`dirname "$0"`/.include"
cd ..
./gradlew clean installer
cd concourse-server/build/distributions
sh concourse-server*bin

exit 0
