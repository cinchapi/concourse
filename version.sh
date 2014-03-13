#!/usr/bin/env bash

#################################################
###  Script to get or set the current version ###
#################################################

BASE_VERSION_FILE=".version"
JENKINS_HOME="/opt/jenkins"
if [ -w $JENKINS_HOME ]; then
	COUNTER_FILE="$JENKINS_HOME/.counter"
else
	COUNTER_FILE=".counter"
fi

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
			IFS='/'
			PARTS=( $BRANCH )
			EXTRA="-${PARTS[1]}"
			EXTRA=`echo $EXTRA | tr '[:lower:]' '[:upper:]'`
			;;
		release* )
			EXTRA=""
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
		sed -i '' -E "s/[0-9]+\.[0-9]+\.[0-9]+/$NEW_VERSION/g" concourse/README.md
		sed -i '' -E "s/pom.version = '[0-9]+\.[0-9]+\.[0-9]'+/pom.version = '$NEW_VERSION'/g" concourse/build.gradle
		sed -i '' -E "s/pom.version = '[0-9]+\.[0-9]+\.[0-9]'+/pom.version = '$NEW_VERSION'/g" concourse-testing/build.gradle
		echo "The version has been set to $NEW_VERSION"
	else
		echo "Please specify a valid version <major>.<minor>.<patch>"
		exit 1
	fi
fi
exit 0



