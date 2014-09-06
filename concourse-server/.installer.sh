#!/usr/bin/env bash

#####################################################################
###  Script to make a self-extracting install and uggrade scripts ###
#####################################################################
# This script should ONLY be invoked from the Gradle installer task!

# Meta variables
# See http://misc.flogisoft.com/bash/tip_colors_and_formatting for formatting tips
bold=`tput bold`
normal=`tput sgr0`

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

	# --- run upgrade tasks
	echo
	. "bin/.env" # NOTE: The .env script cd's into the parent (actual install) directory
	\$JAVACMD -cp "\$CLASSPATH" org.cinchapi.concourse.server.upgrade.Upgrader
	echo

	cd - >> /dev/null
else
	# run initializer tasks
	echo
	. "bin/.env" # NOTE: The .env script cd's into the parent directory (which is the parent directory of the install directory)
	cd - >> /dev/null
	\$JAVACMD -cp "lib/*" org.cinchapi.concourse.server.upgrade.Initializer
	echo
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
echo "Please type your administrative password to allow the installer to make some (optional) system-wide changes."
sudo -K # clear the sudo creds cash, so user is forced to type in password
sudo touch /usr/local/bin/.jeffnelson # dummy command to see if we can escalate permissions
if [ \$? -ne 0 ]; then
	echo "\$(date +'%T.500') [main] WARN - The installer couldn't place the Concourse scripts on your PATH, but you can run them directly from "\$BASE"/bin".
	echo "\$(date +'%T.500') [main] WARN - The installer couldn't place the Concourse log files in /var/log/concourse, but you can access them directly from "\$BASE"/log".
else
	# symlink to log directory
	sudo rm /var/log/concourse 2>/dev/null
	sudo ln -s \$BASE"/log/" /var/log/concourse
	echo "\$(date +'%T.500') [main] INFO - Access the Concourse log files in /var/log/concourse"
	# delete dummy file
	sudo rm /usr/local/bin/.jeffnelson
	# -- concourse
	BINARY=\$BASE"/bin/concourse"
	ARGS=\$(echo '"\$@"')
# NOTE: The section below cannot be indented!
sudo cat << JEFFNELSON > /usr/local/bin/concourse
#!/usr/bin/env bash
sh \$BINARY \$ARGS
exit 0
JEFFNELSON
	sudo chmod +x /usr/local/bin/concourse
	sudo chown \$(whoami) /usr/local/bin/concourse
	echo "\$(date +'%T.500') [main] INFO - Use 'concourse' to manage Concourse Server"

	# -- cash
	BINARY=\$BASE"/bin/cash"
	ARGS=\$(echo '"\$@"')
# NOTE: The section below cannot be indented!
sudo cat << ASHLEAHGILMORE > /usr/local/bin/cash
#!/usr/bin/env bash
sh \$BINARY \$ARGS
exit 0
ASHLEAHGILMORE
	sudo chmod +x /usr/local/bin/cash
	sudo chown \$(whoami) /usr/local/bin/cash
	echo "\$(date +'%T.500') [main] INFO - Use 'cash' to launch the Concourse Action SHell"
fi

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
../makeself/makeself.sh --notemp --nox11 $DISTS/concourse-server $INSTALLER "Concourse Server" ./$SCRIPT_NAME
chmod +x $INSTALLER
mv $INSTALLER $DISTS
cd $DISTS
rm -rf concourse-server
cd - >> /dev/null

exit 0
