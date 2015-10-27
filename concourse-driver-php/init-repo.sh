#!/usr/bin/env bash
# Initialize this repo

# Ensure the script operates from the project root directory
cd "${0%/*}"
this=`basename "$0"`

# Install the composer dependencies
./composer.phar install

# Delete this script so it isn't run again
git update-index --assume-unchanged $this
rm $this
exit 0
