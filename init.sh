#!/usr/bin/env bash
# This script will do one time initialization in the repo and delete itself

printf "sonatypeUsername=\nsonatypePassword=" > gradle.properties
rm $0
exit 0
