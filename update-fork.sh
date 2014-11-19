#!/usr/bin/env bash
# This script will sync a forked repo with the main upstream source
# Changes from the upstream "develop" branch are pulled into the
# local repository and pushed to the fork.

# Check to make sure this script is not being run from a non-fork
FORK=`git remote -v | grep "origin.*cinchapi/"`
if [ ! -z "$FORK" ]; then
	echo "There is no need to sync a non-forked repo"
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

	git stash
	git pull upstream develop
	git push origin HEAD
	git stash pop
	exit 0
fi