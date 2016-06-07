#!/usr/bin/env bash

# Copyright (c) 2015-2016 Cinchapi Inc.
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

#
# This script will sync a forked repo with the main upstream source
# Changes from the upstream branch with the same name as the local one
# are pulled into the local repository and pushed to the fork.

# Change to the directory of the repo
DIR=`dirname $0`
cd $DIR

# Check to make sure this script is not being run from a non-fork
FORK=`git remote -v | grep "origin.*cinchapi/"`
if [ ! -z "$FORK" ]; then
	echo "There is no need to sync a non-forked repo!"
	exit 128
else
	REMOTE_NAME="upstream"
	REPO_NAME=$(basename `git rev-parse --show-toplevel`)

	# check to see if the upstream exists
	EXISTS=`git remote -v | grep $REMOTE_NAME`
	if [ -z "$EXISTS" ]; then
		echo "Configuring a remote URL to point to the $REMOTE_NAME $REPO_NAME repo"
		git remote add $REMOTE_NAME https://github.com/cinchapi/$REPO_NAME.git
	fi
	BRANCH=`git rev-parse --abbrev-ref HEAD`
	STASH=`git stash`
	echo $STASH
	git fetch upstream
	git pull --no-edit upstream $BRANCH
	git push origin HEAD
	if [ "$STASH" != "No local changes to save" ]; then
		git stash pop
	fi
	cd - > /dev/null
	exit 0
fi
