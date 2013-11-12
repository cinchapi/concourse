#!/usr/bin/env bash

#################################################
###  Script to get or set the current version ###
#################################################

BASE_VERSION_FILE=".version"
COUNTER_FILE=".counter"

if [ -z "$1" ] ; then 
	VERSION=`cat $BASE_VERSION_FILE`
	BRANCH=`git rev-parse --abbrev-ref HEAD`
	COMMIT=`git rev-parse HEAD | cut -c1-10`

	# Get counter value
	if [ ! -f $COUNTER_FILE ] ; then
		echo 0 > $COUNTER_FILE
	fi
	COUNTER=`cat $COUNTER_FILE`
	((COUNTER++))
	echo $COUNTER > $COUNTER_FILE
	VERSION=$VERSION.$COUNTER
	case $BRANCH in 
		develop )
			EXTRA="-SNAPSHOT"
			;;
		feature* )
			EXTRA="-EXPERIMENTAL"
			;;
		release* )
			EXTRA="-BETA"
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
	if [[ $NEW_VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]$ ]] ; then
		echo $NEW_VERSION > $BASE_VERSION_FILE
		rm $COUNTER_FILE
		sed -i '' -E "s/[0-9]+\.[0-9]+\.[0-9]+/$NEW_VERSION/g" README.md
		echo "The version has been set to $NEW_VERSION"
	else
		echo "Please specify a valid version <major>.<minor>.<patch>"
		exit 1
	fi
fi
exit 0



