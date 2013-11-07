#!/usr/bin/env bash

# This config will setup all the enviornment variables and check that
# pthats are proper
. "`dirname "$0"`/.env.sh"

# run the program
exec $JAVACMD -classpath "$CLASSPATH" org.cinchapi.concourse.server.cli.DumpCli "$@"
