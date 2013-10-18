#!/usr/bin/env bash
# This script sets the version number

usage(){
	echo "Usage: $0 <major>.<minor>.<patch>"
	exit -1
}

# Update README.md


# Update build.gradle
sed -i '' 's/version = '\''[0-9]*\.[0-9]*\.[0-9]*'\''/version = '\''0.1.0'\''/g' build.gradle