#!/usr/bin/env bash

#####################################################################
###  Script to make a self-extracting install and uggrade scripts ###
#####################################################################
# This script should ONLY be invoked from the Gradle installer task!

# Gradle passes the version number of the current build to this script
# for uniformity in naming conventions.
VERSION=$1

# This script assumes that it is running from the root of the concourse-server
# project
DISTS="build/distributions"
cd $DISTS
unzip concourse-server*zip
cd - >> /dev/null

SCRIPT_NAME=".update"
SCRIPT="$DISTS/concourse-server/$SCRIPT_NAME"

# We dynamically create an "update" script that copies certain files from
# the new distribution to the current install directory. Afterwards, the
# update script will start the server and run the upgrade task
echo "#!/usr/bin/env bash" >> $SCRIPT

# --- check if we should upgrade by detecting if the directory above is an
# --- installation diretory
echo "files=\$(ls ../lib/concourse*.jar 2> /dev/null | wc -l)" >> $SCRIPT

# --- copy upgrade files
echo "if [ \$files -gt 0 ]; then" >> $SCRIPT #start upgrade
echo "echo 'Upgrading Concourse Server.........................................................................'" >> $SCRIPT
echo "rm -r ../lib/" >> $SCRIPT
echo "cp -fR lib/ ../lib/" >> $SCRIPT
echo "rm -r ../licenses/" >> $SCRIPT
echo "cp -fR licenses/ ../licenses/" >> $SCRIPT
echo "cp -R bin/ ../bin/" >> $SCRIPT
# TODO: Copy config files???

# --- run upgrade task
echo "cd .." >> $SCRIPT
# TODO exec bin/start
# TODO exec bin/upgrade
# TODO exec bin/stop

echo "cd - >> /dev/null" >> $SCRIPT
echo "fi" >> $SCRIPT #end upgrade

# -- delete the update file and installer
echo "rm $SCRIPT_NAME" >> $SCRIPT
echo "cd .." >> $SCRIPT
echo "rm concourse-server*bin" >> $SCRIPT

# --- delete upgrade directory
echo "if [ \$files -gt 0 ]; then" >> $SCRIPT
echo "rm -r concourse-server" >> $SCRIPT
echo "fi" >> $SCRIPT

echo "exit 0" >> $SCRIPT

# Make update script executable
chmod +x $SCRIPT

# Create the installer package
INSTALLER="concourse-server-$VERSION.bin"
../makeself/makeself.sh --notemp $DISTS/concourse-server $INSTALLER "Concourse Server" ./$SCRIPT_NAME
chmod +x $INSTALLER
mv $INSTALLER $DISTS
cd $DISTS
rm -rf concourse-server
cd - >> /dev/null

exit 0