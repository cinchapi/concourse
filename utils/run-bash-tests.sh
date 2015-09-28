#!/usr/bin/env bash

# Ensure that this script operates from the directory in which it resides
cd "${0%/*}"

./bats/bin/bats ../concourse-bash-tests/*
