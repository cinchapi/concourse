#!/usr/bin/env bash
# Script to print context info in the project

FILE="context.txt";
rm $FILE 2>/dev/null
cat <(echo "Last Commit:" & git rev-parse HEAD) <(git status) <(git diff ../*) > $FILE
exit 0
