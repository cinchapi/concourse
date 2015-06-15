#!/usr/bin/env bash

# Ensure the script operates from the project root directory
cd "${0%/*}"

# Delete old files
rm -r build 2>/dev/null
rm -r dist 2>/dev/null
rm -r concourse_driver_python.egg-info 2>/dev/null

# Run the unit tests
nosetests
if [ $? -eq 0 ]; then
  python setup.py clean sdist bdist_wheel
  #twine upload dist/*
  exit 0
fi

exit 1
