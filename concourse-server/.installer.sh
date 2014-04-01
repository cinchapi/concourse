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
cat << EOF > $SCRIPT
#!/usr/bin/env bash

# --- check if we should upgrade by detecting if the directory above is an
# --- installation diretory
files=\$(ls ../lib/concourse*.jar 2> /dev/null | wc -l)

# --- copy upgrade files
if [ \$files -gt 0 ]; then 
	echo 'Upgrading Concourse Server..........................................................................'
	rm -r ../lib/
	cp -fR lib/ ../lib/
	rm -r ../licenses/ 2>/dev/null
	cp -fR licenses/ ../licenses/
	cp -R bin/* ../bin/ # do not delete old bin dir incase it has custom scripts
	rm ../wrapper-linux-x86-64 2>/dev/null # exists prior to 0.3.3
        rm ../wrapper-macosx-universal-64 2>/dev/null # exists prior to 0.3.3
        mkdir -p ../wrapper
        cp -R wrapper/* ../wrapper
	cp -f conf/.concourse.conf ../conf/.concourse.conf

	# --- run upgrade task
	cd ..
	# TODO exec bin/start
	# TODO exec bin/upgrade
	# TODO exec bin/stop

	cd - >> /dev/null
fi

# -- delete the update file and installer
rm $SCRIPT_NAME

# -- install scripts on the path
BASE=\$(pwd)
if [ \$files -gt 0 ]; then
        cd ..
	BASE=\$(pwd)
	cd - >> /dev/null
fi
# -- concourse
BINARY=\$BASE"/bin/concourse"
sudo touch /usr/local/bin/concourse #run without -n flag to prompt for password once
if [ \$? -ne 0 ]; then
	echo "Unable to install the Concourse scripts on your PATH, but you can run them directly from "\$BASE"/bin"
fi
ARGS=\$(echo '"\$@"')
sudo -n cat << JEFFNELSON > /usr/local/bin/concourse 2>/dev/null
#!/usr/bin/env bash
sh \$BINARY \$ARGS
exit 0
JEFFNELSON
sudo -n chmod +x /usr/local/bin/concourse 2>/dev/null
sudo -n chown \$(whoami) /usr/local/bin/concourse 2>/dev/null

# -- cash
BINARY=\$(pwd)
BINARY=\$BASE"/bin/cash"
sudo -n touch /usr/local/bin/cash 2>/dev/null
ARGS=\$(echo '"\$@"')
sudo -n cat << ASHLEAHGILMORE > /usr/local/bin/cash 2>/dev/null
#!/usr/bin/env bash
sh \$BINARY \$ARGS
exit 0
ASHLEAHGILMORE
sudo -n chmod +x /usr/local/bin/cash 2>/dev/null
sudo -n chown \$(whoami) /usr/local/bin/cash 2>/dev/null

cd ..
rm concourse-server*bin

# --- delete upgrade directory
if [ \$files -gt 0 ]; then
	rm -r concourse-server
fi

exit 0
EOF

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
