#!/usr/bin/env bash
# Initialize this repo

# Ensure the script operates from the project root directory
cd "${0%/*}"

# Install the composer dependencies
./composer.phar install
