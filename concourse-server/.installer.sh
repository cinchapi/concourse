#!/usr/bin/env bash

#####################################################################
###  Script to make a self-extracting install and uggrade scripts ###
#####################################################################
# This script should ONLY be invoked from the Gradle installer task!

# Meta variables
# See http://stackoverflow.com/questions/5947742/how-to-change-the-output-color-of-echo-in-linux for formatting tips
ESC="\033["
TEXT_COLOR_RED=$ESC"0;31m"
TEXT_COLOR_RED_BOLD=$ESC"1;31m"
TEXT_COLOR_GREEN=$ESC"0;32m"
TEXT_COLOR_GREEN_BOLD=$ESC"1;32m"
TEXT_COLOR_BLUE=$ESC"0;34m"
TEXT_COLOR_BLUE_BOLD=$ESC"1;34m"
TEXT_COLOR_PURPLE=$ESC"0;35m"
TEXT_COLOR_PURPLE_BOLD=$ESC"1;35m"
TEXT_COLOR_YELLOW=$ESC"1;33m"
TEXT_COLOR_RESET=$ESC"0;m"

# Gradle passes the version number of the current build to this script
# for uniformity in naming conventions.
VERSION=$1

# This is the version that is used by the upgrade framework for determing
# relative installer ordering when trying to perform an upgrade.
UPVERSION=`date +%s`

# This script assumes that it is running from the root of the concourse-server
# project
DISTS="build/distributions"
cd $DISTS
unzip concourse-server*zip
cd - >> /dev/null

# Write metadata that should be copied to the install directory
META_FILE=".mchammer"
METADATA="$DISTS/concourse-server/$META_FILE" # can't touch this
cat << EOF >> $METADATA
Upgrade-Version:    $UPVERSION
EOF

SCRIPT_NAME=".update"
SCRIPT="$DISTS/concourse-server/$SCRIPT_NAME"

# We dynamically create an "update" script that copies certain files from
# the new distribution to the current install directory and possibly integrate
# the server's script with the rest of the system.
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
	rm -r ../LICENSE
	cp -fR LICENSE ../
	cp -fR NOTICE ../ # introduced in 0.5.0
	cp -fR $META_FILE ../ # introduced in 0.5.0
	cp -R bin/* ../bin/ # do not delete old bin dir incase it has custom scripts
	rm ../wrapper-linux-x86-64 2>/dev/null # exists prior to 0.3.3
	rm ../wrapper-macosx-universal-64 2>/dev/null # exists prior to 0.3.3
	rm -rf ../wrapper 2>/dev/null #exists prior to 0.5.0
	rm -rf ../conf/.concourse.conf 2>/dev/null #exists prior to 0.5.0
	rm ../.douge 2>/dev/null # CON-622: Prevent upgrade tasks from being skipped if server was never started prior to upgrade
else
	# Indicate that this is a brand new installation
	touch .douge #fresh
fi

# -- delete the update file and installer
rm $SCRIPT_NAME

# -- Install scripts onto the $PATH
# We add wrapper files that invoke scripts in the bin directory instead of
# copying or symlinking them directly. This allows us to add logic to the
# wrapper scripts to detect when the installation is not valid and potentially
# self correct.
BASE=\$(pwd)
if [ \$files -gt 0 ]; then
        cd ..
	BASE=\$(pwd)
	cd - >> /dev/null
fi
if [[ \$@ != *skip-integration* ]]; then
	echo -e "${TEXT_COLOR_BLUE_BOLD}Please type your password to allow the installer to make some (optional) system-wide changes.${TEXT_COLOR_RESET}"
	sudo -K # clear the sudo creds cash, so user is forced to type in password
	sudo touch /usr/local/bin/.jeffnelson # dummy command to see if we can escalate permissions
	if [ \$? -ne 0 ]; then
		echo -e "${TEXT_COLOR_YELLOW}\$(date +'%T.500') [main] WARN - The installer couldn't place the Concourse scripts on your PATH, but you can run them directly from "\$BASE"/bin${TEXT_COLOR_RESET}"
		echo -e "${TEXT_COLOR_YELLOW}\$(date +'%T.500') [main] WARN - The installer couldn't place the Concourse log files in /var/log/concourse, but you can access them directly from "\$BASE"/log${TEXT_COLOR_RESET}"
	else # BEGIN LOGIC TO INTEGRATE WITH SYSTEM
		# symlink to log directory
		sudo rm /var/log/concourse 2>/dev/null
		sudo ln -sf \$BASE"/log/" /var/log/concourse
		echo -e "${TEXT_COLOR_GREEN}Access the Concourse log files in /var/log/concourse${TEXT_COLOR_RESET}"
		# delete dummy file
		sudo rm /usr/local/bin/.jeffnelson

		# -- Add "concourse" control script to the PATH
		BINARY=\$BASE"/bin/concourse"
		ARGS=\$(echo '"\$@"')
		sudo touch /usr/local/bin/concourse
		sudo chown \$(whoami) /usr/local/bin/concourse
		sudo chmod +x /usr/local/bin/concourse
# ------------------------------------------------------------------------------
# NOTE: This section cannot be indented!
sudo cat << JEFFNELSON > /usr/local/bin/concourse
#!/usr/bin/env bash

# Copyright (c) 2015 Cinchapi Inc.
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

if [ -x \$BINARY ]; then
    \$BINARY \$ARGS
    exit \\\$?
else
    echo -e "${TEXT_COLOR_RED}Whoops! It looks like Concourse is no longer installed. Visit https://concoursedb.com/download or contact Cinchapi support.${TEXT_COLOR_RESET}"
    exit 1
fi
### BEGIN INIT INFO
# Provides: Concourse Server
# Required-Start: $local_fs $network $syslog
# Should-Start:
# Required-Stop:
# Default-Start: 2 3 4 5
# Default-Stop: 0 1 6
# Short-Description: Concourse Server
# Description: Concourse is a self-tuning database designed for both transactions and ad-hoc analytics across time.
### END INIT INFO
JEFFNELSON
# ------------------------------------------------------------------------------
		echo -e "${TEXT_COLOR_GREEN}Use 'concourse' to manage Concourse Server${TEXT_COLOR_RESET}"

		# -- Add "cash" launch script to the PATH
		BINARY=\$BASE"/bin/cash"
		ARGS=\$(echo '"\$@"')
		sudo touch /usr/local/bin/cash
		sudo chown \$(whoami) /usr/local/bin/cash
		sudo chmod +x /usr/local/bin/cash
# ------------------------------------------------------------------------------
# NOTE: This section cannot be indented!
sudo cat << ASHLEAHGILMORE > /usr/local/bin/cash
#!/usr/bin/env bash

# Copyright (c) 2015 Cinchapi Inc.
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

if [ -x \$BINARY ]; then
    \$BINARY \$ARGS
    exit \\\$?
else
    echo -e "${TEXT_COLOR_RED}Whoops! It looks like Concourse is no longer installed. Visit https://concoursedb.com/download or contact Cinchapi support.${TEXT_COLOR_RESET}"
    exit 1
fi
ASHLEAHGILMORE
# ------------------------------------------------------------------------------
		echo -e "${TEXT_COLOR_GREEN}Use 'cash' to launch the Concourse Action SHell${TEXT_COLOR_RESET}"

		# Install Concourse Server as a service
		if [ \$(uname -s) == "Linux" ]; then
			sudo ln -sf /usr/local/bin/concourse /etc/init.d/concourse
		fi
	fi # END LOGIC TO INTEGRATE WITH SYSTEM
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

# Create an LSM file in a temporary location
LSM=`mktemp -t "XXXXXXXXXXXXXX"`
cat << EOF > $LSM
Begin3
Title:              Concourse
Version:            $VERSION
Upgrade-Version:    $UPVERSION
Description:        Concourse is a self-tuning database designed for both
                    transactions and ad hoc analytics across time.
Author:             Cinchapi Inc. (oss@cinchapi.com)
Maintained-by:      Cinchapi Inc. (oss@cinchapi.com)
Original-site:      http://concoursedb.com
Platform:           Unix
Copying-policy:     Apache License, Version 2.0
End
EOF

# Create the installer package
INSTALLER="concourse-server-$VERSION.bin"
../makeself/makeself.sh --lsm $LSM --notemp --nox11 $DISTS/concourse-server $INSTALLER "Concourse Server" ./$SCRIPT_NAME
chmod +x $INSTALLER
mv $INSTALLER $DISTS
cd $DISTS
rm -rf concourse-server
cd - >> /dev/null

exit 0
