#!/usr/bin/env bash

#####################################################################
###  Script to make a self-extracting install and uggrade scripts ###
#####################################################################
# This script should ONLY be invoked from the Gradle dist task!

# Gradle passes the version number of the current build to this script
# for uniformity in naming conventions.
VERSION=$1

# This script assumes that it is running from the root of the concourse-server
# project
DISTS="build/distributions"
cd $DISTS
unzip concourse-server*zip
cd -

########################################################################
############################ INSTALLER #################################
########################################################################

INSTALLER="concourse-server-$VERSION.install"
../makeself/makeself.sh --notemp $DISTS/concourse-server $INSTALLER "Concourse Server"
mv $INSTALLER $DISTS

########################################################################
############################# UPGRADER #################################
########################################################################

SCRIPT_NAME=".update"
SCRIPT="$DISTS/concourse-server/$SCRIPT_NAME"

# We dynamically create an "update" script that copies certain files from
# the new distribution to the current install directory. Afterwards, the
# update script will start the server and run the upgrade task

# --- copy files
echo "#!/usr/bin/env bash" >> $SCRIPT
echo "cp -R lib/ ../lib/" >> $SCRIPT
echo "cp -R bin/ ../bin/" >> $SCRIPT
echo "cp -R licenses/ ../licenses/" >> $SCRIPT
# TODO: Copy config files???
echo "cd .." >> $SCRIPT

# --- run upgrade task
# TODO exec bin/start
# TODO exec bin/upgrade
# TODO exec bin/stop

# --- delete upgrade working files
echo "rm -r concourse-server" >> $SCRIPT
echo "rm concourse-server*upgrade" >> $SCRIPT
echo "exit 0" >> $SCRIPT

# Make upgrade script executable
chmod +x $SCRIPT

UPGRADER="concourse-server-$VERSION.upgrade"
../makeself/makeself.sh --notemp $DISTS/concourse-server $UPGRADER "Concourse Server" ./$SCRIPT_NAME
mv $UPGRADER $DISTS

########################################################################
############################# #CLEANUP #################################
########################################################################
cd $DISTS
rm -rf concourse-server
cd -

exit 0